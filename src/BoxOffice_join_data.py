import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.utils import AnalysisException

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("BoxOfficeJoinDatas") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

sc = SparkContext(conf=spark_conf)

spark = SparkSession(sc)

date = sys.argv[1]

#temp에 떨어진 data를 기반으로 warehouse에 떨어뜨리는데
#요구사항 년도 기반으로 폴더 디렉토리 지정
#한개의 parquet 파일 중, 지역코드를 기반으로 partitioning 

try:
	#오늘 데이터 temp 읽어오기
	s3_append_df = f"s3a://sms-warehouse/temp/kobis/boxOffice_{date}"
	append_df = spark.read.parquet(s3_append_dir)

	#기존 합칠 데이터 읽어오기
	base_data_dir = f"s3a://sms-warehouse/kobis/{date.split('-')[0]}/boxOffice_{date.split('-')[1]}"
    base_df = spark.read.parquet(base_data_dir).cache()

    #데이터 병합
    result_df = base_df.union(append_df)

    result.show()
    result_df.write.mode("overwrite").partitionBy("loc_code").parquet(f"s3a://sms-warehouse/kobis/\
    	{date.split('-')[0]}/boxOffice_{date.split('-')[1]}")

except AnalysisException as e:
	s3_append_df = f"s3a://sms-warehouse/temp/kobis/boxOffice_{date}"
	append_df = spark.read.parquet(s3_append_dir)
    append_df.write.mode("overwrite").partitionBy("loc_code").\
    parquet(f"s3a://sms-warehouse/kobis/{date.split('-')[0]}/\
    boxOffice_{date.split('-')[1]}")


