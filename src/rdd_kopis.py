from lib.modules import *
import io
import json
from xml_to_dict import XMLtoDict
import sys

from pyspark.sql import SparkSession

# Spark Session Build
spark = SparkSession.builder.getOrCreate('SPARK_KOPIS')
print("spark session built successfully")

def get_raw_from_s3(date):
    year = date.split('-')[0]
    print(date,year)
    s3 = create_s3client()
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket='sms-basket',Prefix=f'kopis/{year}')

    file_list = []

    for page in page_iterator :
        file_list += [obj['Key'] for obj in page['Contents'] if obj['Key'].find(date)>-1]

    xml_file_list=[]

    for file in file_list:
        obj=s3.get_object(Bucket='sms-basket', Key = file)

        result=io.BytesIO(obj['Body'].read())
        wrapper = io.TextIOWrapper(result, encoding='utf-8')
        text_xml = wrapper.read()

        parsing_info=XMLtoDict().parse(text_xml)['dbs']['db']
        parsing_json=json.dumps(parsing_info, ensure_ascii=False, indent=2, separators=(',', ': '))
        xml_file_list.append(parsing_json)
        # xml_file_list.append(str(parsing_info))

    return xml_file_list


# 내장 함수
def transform_kopis_json(json_str):
    data = json.loads(json_str)

    try :
        tksite_temp = data.get('tksites').get('tksite')

        if type(tksite_temp) == type([]):
            tksite_pro=[{tksite['#text']:tksite['@href']} for tksite in tksite_temp]
        else:
            tksite_pro=[{tksite_temp['#text']:tksite_temp['@href']}]

    except:
        tksite_pro=[]

    try:
        styurl = list(data.get('styurls').values())

        if type(styurl[0]) == type([]) :
            styurl=sum(styurl,[])

    except :
        styurl=[]

    data['styurls'] = str(styurl)
    data['tksites'] = str(tksite_pro)

    return json.dumps(data)


date = sys.argv[1]

file_list = get_raw_from_s3(date)

# 데이터를 RDD로 변환
raw_rdd = spark.sparkContext.parallelize(file_list)
transformed_rdd = raw_rdd.map(transform_kopis_json)

# 변환된 JSON 데이터 출력
json_df = spark.read.json(transformed_rdd)
json_df.show()

#json_df = json_df.coalesce(1)
out_buffer = io.BytesIO()
pdf = json_df.toPandas()
pdf.to_parquet(out_buffer)

# Parquet 파일로 로컬에 저장 
#output_path = f"/home/ubuntu/spark/spark-3.2.4/test/datas/temp_KOPIS_{date}.parquet"
#json_df.write.parquet(output_path)

s3 =  create_s3client()
output_path = f'test-spark/KOPIS_{date}.parquet'
s3.put_object(Bucket='sms-basket', Key = output_path, Body = out_buffer.getvalue())

spark.stop()