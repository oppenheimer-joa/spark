import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToCreditRdd") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

def get_TMDB_data(file_key):
    if file_key == "wrong":
        return "wrong_category"
    else:
        raw_list=[]
        s3_path = f's3a://sms-basket/{file_key}'
        file_list = spark.sparkContext.wholeTextFiles(s3_path)

        for file in file_list.collect():
            contents=json.loads(file[1])
            raw_list.append(json.dumps(contents))

        print(raw_list)

        return raw_list

#credit 전처리 함수
def transform_TMDB_credit_json(json_data):
    try:
        data = json.loads(json_data)

        crews = data.get("crew", [])
        crew_ids = []

        if len(crews) == 0:
            pass

        else:
            for i in range(len(crews)):
                crew_ids.append(crews[i]["id"])
            data["crew"] = crew_ids

        casts = data.get("cast", [])
        cast_ids = []

        if len(casts) == 0:
            pass

        else:
            for j in range(len(casts)):
                cast_ids.append(casts[j]["id"])
            data["cast"] = cast_ids

        return data
    except json.JSONDecodeError as e:
        return f"wrong code :  {e}"


# date = '1960-01-01'
# Airflow 에서 받을 파라미터

date = sys.argv[1]
category = 'credit'

credit_path = make_tmdb_file_dir(category, date)
credit_data=get_TMDB_data(credit_path)
raw_credit_rdd = spark.sparkContext.parallelize(credit_data)
transformed_credit_rdd = raw_credit_rdd.map(transform_TMDB_credit_json)
json_df = spark.read.json(transformed_credit_rdd)
# json_df.show()

#데이터 프레임을 Parquet 파일로 저장
s3_path = f's3a://sms-warehouse/temp'
filename = f'credit_{date}'

json_df.write.parquet(f"{s3_path}/{filename}")