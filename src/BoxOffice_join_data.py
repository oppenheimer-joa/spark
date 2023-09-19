import sys
sys.path.append('/home/spark/spark_code')
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
    .config('spark.sql.sources.partitionColumnTypeInference.enabled', 'false') \
    .getOrCreate()

date = sys.argv[1]

#temp에 떨어진 data를 기반으로 warehouse에 떨어뜨리는데
#요구사항 년도 기반으로 폴더 디렉토리 지정
#한개의 parquet 파일 중, 지역코드를 기반으로 partitioning 
year = date.split('-')[0]
month = date.split('-')[1]

try:
    #오늘 데이터 temp 읽어오기
    s3_append_dir = f"s3a://sms-warehouse/temp/kobis/boxOffice_{date}"
    append_df = spark.read.parquet(s3_append_dir)
    base_data_dir = f"s3a://sms-warehouse/kobis/{year}/boxOffice_{month}"

    base_df = spark.read.parquet(base_data_dir).cache()
    base_df = base_df.select("loc_code","date", "rank", "movie_nm", "movie_open","sales_amount",
                            "sales_share", "sales_inten", "sales_change", "sales_acc", "audi_cnt", 
                            "audi_inten", "audi_change", "audi_acc", "scrn_cnt", "show_cnt")
    append_df = append_df.select("loc_code","date", "rank", "movie_nm", "movie_open","sales_amount",
                                "sales_share", "sales_inten", "sales_change", "sales_acc", "audi_cnt", 
                                "audi_inten", "audi_change", "audi_acc", "scrn_cnt", "show_cnt")
    #데이터 병합
    result_df = base_df.union(append_df)
    print(base_df.count(), result_df.count()) 
    result_df.show()
    result_df.write.mode("overwrite").partitionBy("loc_code").parquet(f"s3a://sms-warehouse/kobis/{year}/boxOffice_{month}")

except AnalysisException as e:
    s3_append_dir = f"s3a://sms-warehouse/temp/kobis/boxOffice_{date}"
    append_df = spark.read.parquet(s3_append_dir)
    append_df.write.mode("overwrite").partitionBy("loc_code").parquet(f"s3a://sms-warehouse/kobis/{year}/boxOffice_{month}")
