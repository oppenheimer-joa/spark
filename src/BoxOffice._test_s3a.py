#import findspark
#findspark.init()

#import pyspark
#findspark.find()

# from lib.modules import *
from modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import json

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')
#.config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
# Spark session 초기화
spark = SparkSession.builder \
    .appName("JSON to DF") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

# 박스오피스 해당 연도의 해당날짜의 목록 리스트로 반환 - date는 YYYYMMDD 형식으로 받기
def load_s3_all_files_in_dir(date):
    year = date[:4]
    s3 = create_s3client()
    file_list = []

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket='sms-basket', Prefix=f'kobis/{year}/'):
        if "Contents" in page:
            for obj in page['Contents']:
                file_date = obj['Key'].split('/')[-1].split('_')[0]
                if date == file_date:
                    file_list.append(obj['Key'])

    return file_list, year

# s3 json 데이터를 spark dataframe으로 변환 및 가공
def create_df(file_path, areaCd):
    s3 = create_s3client()

    s3_object = s3.get_object(Bucket='sms-basket', Key=file_path)

    raw_data = s3_object['Body'].read()
    str_data = raw_data.decode('utf-8')
    json_data = json.loads(str_data)
    boxoffice_list = json_data["boxOfficeResult"]["dailyBoxOfficeList"]

    # JSON DATAFRAME 변환
    df = spark.createDataFrame(boxoffice_list)
    df = df.withColumn("areaCd", lit(areaCd))
    df = df.withColumn("rank", col("rank").cast(IntegerType()))
    df = df.orderBy("rank")
    df = df.select('areaCd', 'rnum', 'rank', 'rankInten', 'rankOldAndNew', 'movieCd', 'movieNm',
                        'openDt','salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc',
                        'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt')

    return df

# 해당 날짜의 df를 합쳐서 parquet로 s3 업로드
def df_to_parquet(date):
    schema = StructType([
        StructField("areaCd", StringType(), True),
        StructField("rnum", StringType(), True),
        StructField("rank", IntegerType(), True),
        StructField("rankInten", StringType(), True),
        StructField("rankOldAndNew", StringType(), True),
        StructField("movieCd", StringType(), True),
        StructField("movieNm", StringType(), True),
        StructField("openDt", StringType(), True),
        StructField("salesAmt", StringType(), True),
        StructField("salesShare", StringType(), True),
        StructField("salesInten", StringType(), True),
        StructField("salesChange", StringType(), True),
        StructField("salesAcc", StringType(), True),
        StructField("audiCnt", StringType(), True),
        StructField("audiInten", StringType(), True),
        StructField("audiChange", StringType(), True),
        StructField("audiAcc", StringType(), True),
        StructField("scrnCnt", StringType(), True),
        StructField("showCnt", StringType(), True)
    ])
    dataframe = spark.createDataFrame([], schema=schema)

    file_list, year = load_s3_all_files_in_dir(date)

    for file_path in file_list:
        # file_path = 'kobis/2023/20230903_0105002_boxOffice.json'
        areaCd = file_path.split("_")[1]
        temp_df = create_df(file_path, areaCd)
        dataframe = dataframe.union(temp_df)

    dataframe = dataframe.orderBy("areaCd", "rank")

    # s3 저장 경로 - spark/kobis/2023 - 경로 설정 필요
    parquet_path = f's3a://sms-basket/spark/kobis/{year}'
    # 20230903_boxOffice
    filename = f'{date}_boxOffice'
    dataframe.write.parquet(f'{parquet_path}/{filename}')


df_to_parquet('20230904')

# S3에서 Parquet 파일 읽기
input_path = "s3a://sms-basket/spark/kobis/2023/20230904_boxOffice/"
df = spark.read.parquet(input_path)

# 데이터 프레임 내용 출력
df.show()