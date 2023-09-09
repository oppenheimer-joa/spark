# 아니 왜 lib 디렉토리의 모듈이 안 불러와지는 거임????
# from lib.modules import *
from modules import *
from pyspark.sql import SparkSession
import json, sys

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToCreditRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

# date = '1960-01-01'
# movie_code = '1000336'
# category = 'credit'
# Airflow 에서 받을 파라미터
date = sys.argv[1]
movie_code = sys.argv[2]
category = sys.argv[3]

credit_path = make_tmdb_file_dir(category, date, movie_code)
credit_data = get_TMDB_data(credit_path)

raw_credit_rdd = spark.sparkContext.parallelize([credit_data])

#credit 전처리 함수
def transform_TMDB_credit_json(json_data):
    try:
        data = json.loads(json_data)

        crews = data.get("crew", [])
        crew_ids = []

        if len(crews) == 0:
            pass

        else:
            for i in range(len(crews)):
                crew_ids.append(crews[i]["id"])
            data["crew"] = crew_ids

        casts = data.get("cast", [])
        cast_ids = []

        if len(casts) == 0:
            pass

        else:
            for j in range(len(casts)):
                cast_ids.append(casts[j]["id"])
            data["cast"] = cast_ids

        return data
    except json.JSONDecodeError as e:
        return f"wrong code :  {e}"


transformed_credit_rdd = raw_credit_rdd.map(transform_TMDB_credit_json)

# S3에 rdd 데이터 transformed__rdd 저장
s3_path = f's3a://sms-warehouse/temp'
filename = f'credit_{date}_{movie_code}'
transformed_credit_rdd.saveAsTextFile(f"{s3_path}/{filename}")
