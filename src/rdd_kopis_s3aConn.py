from pyspark.sql import SparkSession
import json
from xml_to_dict import XMLtoDict
import sys
from lib.modules import *
# Spark Session Build

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("KOPIS to DF") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
    .getOrCreate()

print("spark session built successfully")

def get_raw_data(date):
    year = date.split('-')[0]

    # date를 포함하는 xml 파일 읽기
    file_list = spark.sparkContext.wholeTextFiles(f"s3a://sms-basket/kopis/{year}/*{date}*.xml")

    parsing_list = []

    # xml -> json string
    for row in file_list.collect():
        xml_string = row[1]
        parsing_info=XMLtoDict().parse(xml_string)['dbs']['db']
        parsing_json=json.dumps(parsing_info, ensure_ascii=False, indent=2, separators=(',', ': '))
        print(parsing_json)
        parsing_list.append(parsing_json)

    return parsing_list

# 데이터 전처리
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

    data['styurls'] = styurl
    data['tksites'] = tksite_pro
    print(data)

    return json.dumps(data)

# spark job

date = sys.argv[1]
file_list = get_raw_data(date)

# 데이터를 RDD로 변환
raw_rdd = spark.sparkContext.parallelize(file_list)
transformed_rdd = raw_rdd.map(transform_json)

# 변환된 JSON 데이터 출력
json_df = spark.read.json(transformed_rdd)
json_df.show()

# 데이터 프레임을 Parquet 파일로 저장
output_path = f'sms-basket/test-spark/KOPIS_{date}.parquet'
json_df.write.parquet(f"s3a://{output_path}")

spark.stop()