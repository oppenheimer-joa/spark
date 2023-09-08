from lib.modules import *
from pyspark.sql import SparkSession
import json

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToImageRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

year = '1960-01-01'
movie_code = '1000336'
category = 'image'

image_path = make_tmdb_file_dir(category, year, movie_code)
image_data = get_TMDB_data(image_path)

raw_image_rdd = spark.sparkContext.parallelize([image_data])
raw_image_rdd.foreach(print)

#image 전처리 함수
def transform_TMDB_image_json(json_data):
    try:
        data = json.loads(json_data)
        posters = data.get("posters", [])
        if posters:
            poster_file_path = posters[0].get("file_path", "")
            data["posters"] = poster_file_path
            
        return data

    except json.JSONDecodeError as e:
        return f"JSON decoding error: {e}"

transformed_image_rdd = raw_image_rdd.map(transform_TMDB_image_json)

#API 서버내에 데이터 rdd 데이터 transformed_image_rdd 저장
transformed_image_rdd.saveAsTextFile(f"/Users/jesse/Documents/sms/spark/image_{year}_{movie_code}")