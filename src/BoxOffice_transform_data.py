import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StructType, StructField, StringType


access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("BoxOfficeJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

#date_foramt : yyyy-mm-dd
date = '2023-07-31'

# 박스오피스 wholeText로 다 가져오고, 거기서 앞에 날짜를 기반으로 해당 json만 읽어오기
s3_path = spark.sparkContext.wholeTextFiles(f"s3a://sms-basket/kobis/{date.split('-')[0]}")
s3_data = s3_path.collect()
print(s3_data)
# 읽은 json 파일 dataframe으로 전부다 합치기 지역코드 추가해야함

# 만들어진 df를 temp에 떨어뜨려야함
