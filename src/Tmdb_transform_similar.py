from lib.modules import *
from pyspark.sql import SparkSession
import json

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToSimilarRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

year = '1960-01-01'
movie_code = '1000336'
category = 'similar'

similar_path = make_tmdb_file_dir(category, year, movie_code)
similar_data = get_TMDB_data(similar_path)

raw_similar_rdd = spark.sparkContext.parallelize([similar_data])

#similar 전처리 함수
def transform_TMDB_similar_json(json_data):
    try:
        data = json.loads(json_data)
        keys_to_remove = ["total_pages", "total_results", "page"]
        
        # key 삭제
        for key in keys_to_remove:
            if key in data:
                del data[key]

        results = data.get("results", [])
        similar_ids = []
        for i in range(5):
            similar_ids.append(results[i]["id"])
        data["results"] = similar_ids
        data["id"] = int(movie_code)
        return data

    except json.JSONDecodeError as e:
        return f"json decode err : {e}"

transformed_similar_rdd = raw_similar_rdd.map(transform_TMDB_similar_json)

#API 서버내에 데이터 rdd 데이터 transformed_image_rdd 저장
transformed_similar_rdd.saveAsTextFile(f"/Users/jesse/Documents/sms/spark/similar_{year}_{movie_code}")