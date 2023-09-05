import findspark
findspark.init()

import pyspark
findspark.find()

from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, explode, lit
from pyspark.sql.types import IntegerType
import json

# Spark session 초기화
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("JSON to DF") \
    .getOrCreate()
    
# 박스오피스 해당 연도의 모든 파일 목록 리스트로 반환
def load_s3_all_files_in_dir(year):
    s3 = create_s3client()
    file_list = []

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket='sms-basket', Prefix=f'kobis/{year}/'):
        if "Contents" in page:
            for obj in page['Contents']:
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

def json_to_parquet_upload(file_path):
    
    # 받는 file_path의 형식 정하기
    # file_path = 'kobis/2023/20230903_0105002_boxOffice.json'
    
    # 20230903_0105002_boxOffice
    filename = file_path.split('/')[-1].split('.')[0]
    # spark/kobis/2023
    parquet_path = f'spark/{"/".join(file_path.split("/")[:2])}'
    
    json_str = s3_file_to_json(file_path)
    json_data = json.loads(json_str)
    boxoffice_list = json_data['boxOfficeResult']['dailyBoxOfficeList']

    # JSON DATAFRAME 변환
    df = spark.createDataFrame(boxoffice_list)
    sorted_df = df.withColumn("rank", col("rank").cast(IntegerType())).orderBy("rank")
    selected_df = sorted_df.select('rnum', 'rank', 'rankInten', 'rankOldAndNew', 'movieCd', 'movieNm', 'openDt', 
                        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 
                        'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt')

    # Parquet 파일로 저장
    selected_df.write.parquet(f'file:///home/kjh/code/SMS/spark/data/{filename}')

    # S3 업로드
    s3 = create_s3client()
    s3.upload_file(f'file:///home/kjh/code/SMS/spark/data/{filename}', 'sms-basket', f'{parquet_path}/{filename}')



