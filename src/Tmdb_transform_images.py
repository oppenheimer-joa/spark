import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToImageDataFrame") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

# date = '1960-01-01'
# movie_code = '1000336'
# Airflow 에서 받을 파라미터
date = sys.argv[1]
category = 'image'

s3_files = spark.sparkContext.wholeTextFiles(f's3a://sms-basket/TMDB/{category}/{date}')

#image 전처리 함수
def transform_TMDB_image_json(json_data):
    try:
        data = json.loads(json_data)
        posters = data.get("posters", [])
        movie_id = data.get("id", "")
        if posters:
            poster_file_path = posters[0].get("file_path", "")
        else:
            poster_file_path = ""
            
        return (movie_id, poster_file_path)

    except json.JSONDecodeError as e:
        return f"JSON decoding error: {e}"

transformed_image_rdd = s3_files.values().map(transform_TMDB_image_json)

image_df = spark.createDataFrame(transformed_image_rdd, ["id", "posters"])

# S3에 rdd 데이터 transformed__rdd 저장
s3_path = f's3a://sms-warehouse/temp'
filename = f'image_{date}'
image_df.write.mode("overwrite").parquet(f'{s3_path}/{filename}')



