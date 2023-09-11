# from lib.modules import *
from tmp_lib import *
from xml_to_dict import XMLtoDict
import json
import io

import findspark
findspark.init()

import pyspark
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
conf.set("parquet.enable.summary-metadata", "false")
conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
conf.set("spark.sql.parquet.writeLegacyFormat", "true")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


def get_raw_data(date):
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
        print(parsing_json)
        xml_file_list.append(parsing_json)
        # xml_file_list.append(str(parsing_info))

    return xml_file_list

def transform_json(json_str):
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

def kopis_spark_job(date):

    file_list = get_raw_data(date)

    # 데이터를 RDD로 변환
    raw_image_rdd = spark.sparkContext.parallelize(file_list)
    print('aaaaaaaaaaaaaaaaa',raw_image_rdd)
    transformed_image_rdd = raw_image_rdd.map(transform_json)

    # 변환된 JSON 데이터 출력
    json_df = spark.read.json(transformed_image_rdd)

    # 데이터프레임 보기
    json_df.select('styurls','tksites').show()

    # Parquet 파일로 저장 - s3 로 경로 수정 필요
    #output_path = f"file:///home/sub/cong/spark/data/KOPIS_{date}.parquet"
    #json_df = json_df.coalesce(1)
    #json_df.write.parquet(output_path)

date='2023-08-28'
kopis_spark_job(date)
spark.stop()