import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession, Row

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("ImdbAcademyWinnersJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

# year = '2022'
# Airflow 에서 받을 파라미터
year = sys.argv[1]
festa_name = 'academy'

academy_path = make_imdb_file_dir(festa_name, year)
academy_data = get_s3_data(academy_path)

def transform_imdb(json_str):
    json_obj = json.loads(json_str)
    awards_list = json_obj["nomineesWidgetModel"]["eventEditionSummary"]["awards"]

    final_data = []

    for i in awards_list:
        award_Name = i["awardName"]
        for cate in i["categories"]:
            for nomi in cate["nominations"]:
                if nomi["isWinner"] is True :
                    award_Category = nomi["categoryName"]

                    if len(nomi["primaryNominees"]) != 0 :
                        award_Winner = nomi["primaryNominees"][0]["name"]
                        award_Image = nomi["primaryNominees"][0]["imageUrl"]
                    else:
                        pass

                    period_tuple = (award_Name, award_Category, award_Winner, award_Image)
                    final_data.append(period_tuple)
                else :
                    pass

    return final_data

# 데이터를 RDD로 변환
raw_imdb_rdd = spark.sparkContext.parallelize([academy_data])
transformed_rdd = raw_imdb_rdd.map(transform_imdb)
tmp_rdd = transformed_rdd.collect()[0]
rdd_rows = [Row(award_name=row[0], award_category=row[1],award_winner=row[2], award_image=row[3]) for row in tmp_rdd]

academy_df = spark.createDataFrame(rdd_rows)
# s3 저장 경로
parquet_path = f's3a://sms-warehouse/imdb/{festa_name}/{year}'
# imdb_academy_1931
filename = f'imdb_{festa_name}_{year}'
academy_df.write.parquet(f'{parquet_path}/{filename}')

spark.stop()
