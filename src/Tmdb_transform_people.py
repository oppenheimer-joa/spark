import json, sys
#sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToPeopleRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

date = '1960-01-22'
code = '999606'

# Airflow 에서 받을 파라미터
#date = sys.argv[1]
#code = sys.argv[2]
category = 'people'

people_path = make_tmdb_file_dir(category, date, code)
people_data = get_TMDB_data(people_path)

raw_people_rdd = spark.sparkContext.parallelize([people_data])

#people 전처리 함수
def transform_TMDB_people_json(json_data):
    try:
        result = []
        data = json.loads(json_data)
        people_name = data.get("name","")
        people_role = data.get("known_for_department","")
        result.append((people_name, people_role))
        return result
    except json.JSONDecodeError as e:
        return (f"json decode err : {e}")


transformed_people_rdd = raw_people_rdd.map(transform_TMDB_people_json)

transformed_people_rdd.foreach(print)

'''
# S3에 rdd 데이터 transformed__rdd 저장

s3_path = f's3a://sms-warehouse/temp'
filename = f'similar_{date}_{movie_code}'
transformed_similar_rdd.saveAsTextFile(f"{s3_path}/{filename}")
'''