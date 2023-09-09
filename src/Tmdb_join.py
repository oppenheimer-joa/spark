from lib.modules import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import json, sys

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark configuration 설정
spark_conf = SparkConf().setAppName("TmdbLoadTempFiles") \
    .set("spark.hadoop.fs.s3a.access.key", access) \
    .set("spark.hadoop.fs.s3a.secret.key", secret) \
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

# SparkContext 초기화
sc = SparkContext(conf=spark_conf)

# SparkSession 초기화
spark = SparkSession(sc)

# date = '1960-01-01'
# movie_code = '1000336'
# categories = ['image','detail', 'credit', 'similar']
# Airflow 에서 받을 파라미터
date = sys.argv[1]
movie_code = sys.argv[2]
categories = ['image', 'detail', 'credit', 'similar']

# 폴더 패스 들어오면 해당 rdd를 받아 반환
def load_tmp_tmdb_data(folder_path):
	tmp_rdd = sc.textFile(folder_path)
	return tmp_rdd.collect()

# rdd -> df
def convert_df_temp_data(json_data):
	print(json_data)
	print(type(json_data))
	df = spark.read.json(sc.parallelize(json_data))
	return df

# 동적 할당한 변수에 읽어온 rdd 할당
for category in categories:
	rdd_var = f"{category}_rdd"
	folder_path = f"s3a://sms-warehouse/temp/{category}_{date}_{movie_code}"
	rdd_data = load_tmp_tmdb_data(folder_path)

	# DataFrame 생성 및 전역 변수에 할당
	df_var = f"{category}_df"
	locals()[df_var] = convert_df_temp_data(rdd_data)

# 동적할당된 변수 다 불러다가 join

result_df = credit_df.join(image_df, on='id', how='inner')\
    .join(detail_df, on='id', how='inner')\
    .join(similar_df, on='id', how='inner')

# s3 저장 경로 
parquet_path = f's3a://sms-warehouse/TMDB/{date}'
# TMDB_movie_1002185_2023-09-08
filename = f'TMDB_movie_{movie_code}_{date}'
result_df.write.parquet(f'{parquet_path}/{filename}')

spark.stop()
