import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession

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

# date = '1960-01-01'
# movie_code = '1000336'
# Airflow 에서 받을 파라미터
date = sys.argv[1]
movie_code = sys.argv[2]
category = 'similar'

similar_path = make_tmdb_file_dir(category, date, movie_code)
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

# S3에 rdd 데이터 transformed__rdd 저장
s3_path = f's3a://sms-warehouse/temp'
filename = f'similar_{date}_{movie_code}'
transformed_similar_rdd.saveAsTextFile(f"{s3_path}/{filename}")