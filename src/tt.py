import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession

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


base_dir = f"s3a://sms-warehouse/TMDB/2023-07-14/TMDB_movie_872585_2023-07-14"
append_df = spark.read.parquet(base_dir)
df = append.select("genre")

desired_genres = {
    "Action": 28,
    "Adventure": 12,
    "Animation": 16,
    "Comedy": 35,
    "Crime": 80,
    "Documentary": 99,
    "Drama" : 18
    "Family" : 10751,
    "Fantasy" : 14
    "History" : 36,
    "Horror" : 27,
    "Music" : 10402,
    "Mystery" : 9648,
    "Romance" : 10749
    "Science Fiction" : 878,
    "TV Movie" : 10770,
    "Thriller" : 53,
    "War" : 10752,
    "Western" : 37
}

# 각 장르에 대한 컬럼 생성
for genre, genre_code in desired_genres.items():
    # 장르가 해당 영화에 속하는지 여부를 확인하여 1 또는 0 할당
    df = df.withColumn(genre, when(col("genres").contains(genre_code), 1).otherwise(0))

# "genres" 컬럼 제거
df = df.drop("genres")

# 결과 데이터 프레임 출력
df.show() 
