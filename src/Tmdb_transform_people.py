import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date, year

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

date = sys.argv[1]

s3_files = spark.sparkContext.wholeTextFiles(f's3a://sms-basket/TMDB/people/{date}')

# people 전처리 과정
def transform_TMDB_people_json(json_data):
    try:
        data = json.loads(json_data)
        people_id = data.get("id", "")
        people_img = data.get("profile_path", "")
        people_birth = data.get("birthday", "")
        people_death = data.get("deathday", "")
        people_name = data.get("name", "")
        people_role = data.get("known_for_department", "")
        return (people_id, date, people_name, people_role, people_img, people_birth, people_death)
    except json.JSONDecodeError as e:
        return (f"json decode err: {e}")

transformed_people_rdd = s3_files.values().map(transform_TMDB_people_json)

schema = StructType([
    StructField("id", StringType(), True),
    StructField("date_gte", StringType(), True),
    StructField("name", StringType(), True),
    StructField("known_for_department", StringType(), True),
    StructField("profile_img", StringType(), True),
    StructField("birth", StringType(), True),
    StructField("death", StringType(), True),
])

people_df = spark.createDataFrame(transformed_people_rdd, schema)

# S3에 parquet 데이터 저장
s3_path = f's3a://sms-warehouse/temp/people'
filename = f'people_{date}'
people_df.write.mode("overwrite").parquet(f'{s3_path}/{filename}')

