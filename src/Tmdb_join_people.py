import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import to_date
from pyspark.sql.utils import AnalysisException

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

try:
    #이번주temp 전처리 people_data df 읽어오기
    s3_append_dir = f"s3a://sms-warehouse/temp/people/people_{date}"
    append_df = spark.read.parquet(s3_append_dir)

    # 기존 합칠 데이터 append_df 불러오기
    base_data_dir = "s3a://sms-warehouse/TMDB/people_info"
    base_df = spark.read.parquet(base_data_dir)

    # 두 df 합치기
    base_df = base_df.union(append_df)

    # date_gte string -> date
    base_df = df.withColumn("date_gte", to_date(df["date_gte"], "yyyy-MM-dd"))

    # 합친 df를 date_gte 기반 yyyy로만 파티셔닝하여 parquet으로 저장
    base_df = df.withColumn("year", year(base_df["date_gte"]))
    base_df.write.mode("overwrite").partitionBy("year").parquet("s3a://sms-warehouse/TMDB/people_info")

except AnalysisException as e:
    s3_append_dir = f"s3a://sms-warehouse/temp/people/people_{date}"
    append_df = spark.read.parquet(s3_append_dir)
    append_df = df.withColumn("date_gte", to_date(df["date_gte"], "yyyy-MM-dd"))
    append_df = df.withColumn("year", year(base_df["date_gte"]))

    append_df.write.mode("overwrite").partitionBy("year").parquet("s3a://sms-warehouse/TMDB/people_info")






