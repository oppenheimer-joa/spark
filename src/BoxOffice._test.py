import findspark
findspark.init()

import pyspark
findspark.find()

# from lib.modules import *
from modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import json, os

# Spark session 초기화
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("JSON to DF") \
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

    return file_list

# 파일을 json 형식으로 반환
def s3_file_to_json(file_path):

    s3 = create_s3client()

    s3_object = s3.get_object(Bucket='sms-basket', Key=file_path)

    raw_data = s3_object['Body'].read()
    str_data = raw_data.decode('utf-8')
    json_data = json.dumps(json.loads(str_data))
    
    return json_data

def json_to_df(file_path, areaCd):

    # 받는 file_path의 형식 정하기
    # file_path = 'kobis/2023/20230903_0105002_boxOffice.json'
    
    json_str = s3_file_to_json(file_path)
    json_data = json.loads(json_str)
    boxoffice_list = json_data['boxOfficeResult']['dailyBoxOfficeList']

    # JSON DATAFRAME 변환
    df = spark.createDataFrame(boxoffice_list)
    df = df.withColumn("areaCd", lit(areaCd))
    df = df.withColumn("rank", col("rank").cast(IntegerType()))
    df = df.orderBy("rank")
    df = df.select('areaCd', 'rnum', 'rank', 'rankInten', 'rankOldAndNew', 'movieCd', 'movieNm', 'openDt', 
                        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 
                        'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt')

    return df

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
    dataframe = spark.createDataFrame([], schema)

    file_list = load_s3_all_files_in_dir(date)
    
    # 20230903_boxOffice
    filename = f'{date}_boxOffice'
    
    local_path = f'/home/kjh/code/SMS/spark/data/{filename}'
    
    for file_path in file_list:
        areaCd = file_path.split("_")[1]
        temp_df = json_to_df(file_path, areaCd)
        dataframe = dataframe.union(temp_df)
        
    dataframe = dataframe.orderBy("areaCd", "rank")
    
    # Parquet 파일로 저장
    dataframe.write.parquet(f'file:///home/kjh/code/SMS/spark/data/{filename}')

    year = date[:4]
    # spark/kobis/2023
    parquet_path = f'spark/kobis/{year}'
    # S3 업로드
    s3 = create_s3client()
    for file in os.listdir(local_path):
        path = os.path.join(local_path, file)
        s3_path = os.path.join(f'{parquet_path}/{filename}', file)
        s3.upload_file(path, 'sms-basket', s3_path)

df_to_parquet('20230904')
df = spark.read.parquet('file:///home/kjh/code/SMS/spark/data/20230904_boxOffice')
df.show()