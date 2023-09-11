import sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession, Row
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# S3 파일시스템 설정
s3_fs = fs.S3FileSystem(access_key=access, secret_key=secret)
# Arrow 파일 시스템 설정
parrow_filesystem = fs._to_filesystem(s3_fs)


# Spark session 초기화
spark = SparkSession.builder \
    .appName("ImdbWinnersJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()


# s3 경로 설정
s3_path = "s3a://sms-warehouse/imdb/transformed-data"
s3_whole_path_arrow = "s3a://sms-warehouse/imdb/all/arrow/"

# s3 path 하위에 있는 모든 폴더에 존재하는 모든 Parquet 파일을 하나의 데이터프레임으로 읽기
df = spark.read.option("header", "true").parquet(s3_path + "/*/*/*/*.parquet")

# 데이터프레임을 Pandas DataFrame으로 변환
pandas_df = df.toPandas()

# Pandas DataFrame을 Arrow 형식으로 저장하기 전에 festa_name 열을 기준으로 파티션
table = pa.Table.from_pandas(pandas_df)
# 데이터를 저장할 때 디렉토리 구조를 반영하여 객체 키를 설정
partitioned_s3_whole_path_arrow = s3_whole_path_arrow + pandas_df['festa_name'] + '/'
pq.write_to_dataset(table, root_path=partitioned_s3_whole_path_arrow, partition_cols=['festa_name'], compression='snappy', use_dictionary=True, filesystem=parrow_filesystem)

# SparkSession 종료
spark.stop()