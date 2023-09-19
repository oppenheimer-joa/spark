import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToSimilarRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()


def get_TMDB_data_similar(category,date):
    raw_list=[]

    s3_path = f's3a://sms-basket/TMDB/{category}/{date}/*.json'
    file_list = spark.sparkContext.wholeTextFiles(s3_path)

    for file in file_list.collect():
        movieCode = file[0].split("_")[2]  # 파일명에서 movieCode 추출
        contents=json.loads(file[1])
        new_json={'movieCode':movieCode,'contents':contents}
        raw_list.append(json.dumps(new_json))

    return raw_list
    

#similar 전처리 함수
def transform_TMDB_similar_json(json_data):
    try:
        raw = json.loads(json_data)
        data=raw.get('contents')
        movie_code=raw.get('movieCode')

        keys_to_remove = ["total_pages", "total_results", "page"]
        
        # key 삭제
        for key in keys_to_remove:
            if key in data:
                del data[key]

        results = data.get("results", [])
        similar_ids = []

        if len(results) != 0:
            k=min(len(results),5)
            for i in range(k):
                similar_ids.append(results[i]["id"])
            data["results"] = similar_ids
            data["id"] = int(movie_code)
        else :
            data["results"] = similar_ids
            data["id"] = int(movie_code)

        return data

    except json.JSONDecodeError as e:
        return f"json decode err : {e}"


# date = '1960-01-01'
# Airflow 에서 받을 파라미터

date = sys.argv[1]
category = 'similar'

similar_path = make_tmdb_file_dir(category, date)
similar_data = get_TMDB_data_similar(similar_path)
raw_similar_rdd = spark.sparkContext.parallelize(similar_data)
transformed_similar_rdd = raw_similar_rdd.map(transform_TMDB_similar_json)
json_df = spark.read.json(transformed_similar_rdd)
# json_df.show()

#데이터 프레임을 Parquet 파일로 저장
s3_path = f's3a://sms-warehouse/temp'
filename = f'similar_{date}'
json_df.write.parquet(f"{s3_path}/{filename}")


