import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession
import xmltodict

# Spark Session Build

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("KopisXmlToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

print("spark session built successfully")

# date 폴더 내 모든 json 파일 읽어오기
def get_raw_json(date):
    raw_list = []

    s3_path = f's3a://sms-basket/spotify/{date}/*.json'
    file_list = spark.sparkContext.wholeTextFiles(s3_path)

    for file in file_list.collect():
        movieCode = file[0].split("_")[1]  # 파일명에서 movieCode 추출
        contents=json.loads(file[1])

        new_json={'contents':contents,'movieCode':movieCode}
        raw_list.append(json.dumps(new_json))

    #print(raw_list)
    return raw_list

# ['albums']['items'] 내 3개 추출
# movie_id  |  album1  | album2  |  album 3

# raw json 에서 필요 컬럼 추출 
def transform_spotify_json(raw_data):

    data = json.loads(raw_data)
    items=data.get('contents',{}).get('albums',{}).get('items',[])

    transform_json = {}

    for idx, item in enumerate(items):
        item_dict=None
        name = item.get('name', '') # 앨범명
        artists = item.get('artists', [{}])[0].get('name','') # 아티스트명
        url = item.get('external_urls', {}).get('spotify', '') # spotify url
        image = item.get('images', [{}])[0].get('url', '') # 앨범 이미지 url

        item_dict = {'name': name, 'artists': artists, 'url': url, 'image': image}

        transform_json[f'album{idx+1}'] = str(item_dict)

    del data['contents']
    data.update(transform_json) # movie_id  |  album1  | album2  |  album 3

    return json.dumps(data)

# spark job
def spark_job_spotify(date):
    year=date.split('-')[0]
    file_list=get_raw_json(date)
    raw_rdd = spark.sparkContext.parallelize(file_list)
    transformed_rdd = raw_rdd.map(transform_spotify_json)

    json_df = spark.read.json(transformed_rdd)
    json_df.show()

    # 데이터 프레임을 Parquet 파일로 저장
    output_path = f'sms-warehouse/spotify/{year}/{date}'
    # json_df.write.parquet(f"s3a://{output_path}")
    json_df.write.mode("overwrite").parquet(f"s3a://{output_path}")

# Airflow 로부터 받는 변수
date = sys.argv[1]
spark_job_spotify(date)
spark.stop()