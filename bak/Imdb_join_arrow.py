import sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession, Row


access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("ImdbWinnersJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()


# s3 경로 설정
s3_path = "s3a://sms-warehouse/imdb/transformed-data"
s3_whole_path_arrow = "s3a://sms-warehouse/imdb/all/arrow/"

# s3 path 하위에 있는 모든 폴더에 존재하는 모든 Parquet 파일을 하나의 데이터프레임으로 읽기
df = spark.read.option("header", "true").parquet(s3_path + "/*/*/*/*.parquet")

df.coalesce(1).write.format("arrow").partitionBy("festa_name").save(s3_whole_path_arrow)

# SparkSession 종료
spark.stop()