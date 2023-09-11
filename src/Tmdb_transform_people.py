import json, sys
#sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToPeopleRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

date = '1960-01-22'
code = '999606'

# Airflow 에서 받을 파라미터
#date = sys.argv[1]
#code = sys.argv[2]
category = 'people'

people_path = make_tmdb_file_dir(category, date, code)
people_data = get_TMDB_data(people_path)

raw_people_rdd = spark.sparkContext.parallelize([people_data])
raw_people_rdd.foreach(print)

#similar 전처리 함수

'''
transformed_similar_rdd = raw_similar_rdd.map(transform_TMDB_similar_json)

# S3에 rdd 데이터 transformed__rdd 저장

s3_path = f's3a://sms-warehouse/temp'
filename = f'similar_{date}_{movie_code}'
transformed_similar_rdd.saveAsTextFile(f"{s3_path}/{filename}")
'''