import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import to_date, year, col
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark configuration 설정
spark_conf = SparkConf().setAppName("UnionDatas") \
    .set("spark.hadoop.fs.s3a.access.key", access) \
    .set("spark.hadoop.fs.s3a.secret.key", secret) \
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

sc = SparkContext(conf=spark_conf)

spark = SparkSession(sc)

date = sys.argv[1]
# Parquet 파일의 스키마 정의
schema = StructType([
    StructField("id", StringType(), True),  # "id" 열의 데이터 유형을 LongType으로 설정
    StructField("date_gte", StringType(), True),  # "date_gte" 열의 데이터 유형을 DateType으로 설정
    StructField("name", StringType(), True),
    StructField("known_for_department", StringType(), True),
    StructField("profile_img", StringType(), True),
    StructField("birth", StringType(), True),
    StructField("death", StringType(), True)
])

# 기존 합칠 데이터 base_df 불러오기
base_data_dir = "s3a://sms-warehouse/temp/people/*"
base_df = spark.read.schema(schema).parquet(base_data_dir)
# "id" 열을 bigint로 변환
#base_df = base_df.withColumn("id", col("id").cast("bigint"))
base_df = base_df.withColumn("date_gte", to_date(base_df["date_gte"], "yyyy-MM-dd"))
base_df = base_df.withColumn("year", year(base_df["date_gte"])).cache()

base_df.show()
base_df.write.mode("overwrite").partitionBy("year").parquet("s3a://sms-warehouse/TMDB_people")

spark.stop()