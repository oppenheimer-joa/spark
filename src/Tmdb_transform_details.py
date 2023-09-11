import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToDetailRdd") \
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

#date = '1960-01-01'
#movie_code = '1000336'
category = 'detail'

detail_path = make_tmdb_file_dir(category, date, movie_code)
detail_data = get_TMDB_data(detail_path)

raw_detail_rdd = spark.sparkContext.parallelize([detail_data])

#detail 전처리 함수
def transform_TMDB_detail_json(json_data):
    try:
        data = json.loads(json_data)
        keys_to_remove = ["vote_count", "vote_average", "title", "tagline", "status", "popularity","adult"]
        for key in keys_to_remove:
            if key in data:
                del data[key]

        genres = data.get("genres", [])
        genre_ids = [genre["id"] for genre in genres]
        data["genres"] = genre_ids

        return json.dumps(data)
    except json.JSONDecodeError as e:
        return (f"json decode err : {e}")

transformed_detail_rdd = raw_detail_rdd.map(transform_TMDB_detail_json)

# S3에 rdd 데이터 transformed__rdd 저장
s3_path = f's3a://sms-warehouse/temp'
filename = f'detail_{date}_{movie_code}`'
transformed_detail_rdd.saveAsTextFile(f"{s3_path}/{filename}")