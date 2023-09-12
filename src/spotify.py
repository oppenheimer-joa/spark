from pyspark.sql import SparkSession, Row
import json
import sys
from modules import *

# Spark Session Build
access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("KOPIS to DF") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
    .getOrCreate()

print("spark session built successfully")

# Airflow 로부터 받는 변수
date = sys.argv[1]

raw_list = []
rdd_list = []

# date 폴더 내 모든 json 파일 읽어오기
def get_raw_json(date):

    s3_path = f's3a://sms-basket/spotify/{date}/*.json'
    file_list = spark.sparkContext.wholeTextFiles(s3_path)
    for file in file_list.collect() :
        movie_id = file[0].split("_")[1]
        contents = json.dumps(json.loads(file[1])['albums'])
        raw_list.append((movie_id, contents))
    return raw_list


# ['albums']['items'] 내 3개 추출
# movie_id  |  album1  | album2  |  album 3
# album{i} = [ name, external_urls['spotify'], images[0]['url'] ] 

def transform_spotify_json(movie_id, albums) :

    json_data = json.loads(albums)
    data = json_data['albums']['items']
    musics = []

    for item in data :
        name = item['name']
        url = item['external_urls']['spotify']
        image = item['images'][0]['url']
        musics.append([name, url, image])

    rdd_list.append({'movie_id': movie_id, 'album': musics})
    return rdd_list


get_raw_json(date)
for row in raw_list :
    print(row)

rdd = spark.sparkContext.parallelize([raw_list])
json_df = spark.read.json(rdd)
json_df.show()

# 각 요소를 Row 객체로 변환하여 컬럼 이름을 지정한 RDD 생성
raw_rdd = rdd.map(lambda item: Row(movie_id=item['movie_id'], albums=item['albums']))

df = spark.read.json(raw_rdd)
df.show()


# trans_rdd = raw_rdd.map(transform_spotify_json)
# trans_df = spark.read.json(trans_rdd)
# trans_df.show()


# # s3_path = f's3a://sms-warehouse/spotify'
# # raw_rdd.saveAsTextFile(f"{s3_path}/{date}")

# json_df.write.parquet(f's3a://sms-warehouse/spotify/{date}')

spark.stop()
