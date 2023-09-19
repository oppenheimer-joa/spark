import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

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

date = sys.argv[1]
categories = ['image', 'detail', 'credit', 'similar']

# 동적 할당한 변수에 읽어온 rdd 할당
for category in categories:
	df_var = f"{category}_df"
	folder_path = f"s3a://sms-warehouse/temp/{category}_{date}"
	locals()[df_var] = spark.read.parquet(folder_path)

# 동적할당된 변수 다 불러다가 join

result_df = credit_df.join(image_df, on='id', how='inner')\
    .join(detail_df, on='id', how='inner')\
    .join(similar_df, on='id', how='inner')

# s3 저장 경로 
parquet_path = f's3a://sms-warehouse/TMDB/{date}'
# TMDB_movie_1002185_2023-09-08
result_df.write.mode("overwrite").parquet(f'{parquet_path}')
# result_df.write.parquet(f'{parquet_path}')

spark.stop()
