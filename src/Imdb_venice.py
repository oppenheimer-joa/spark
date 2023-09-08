from lib.modules import *
import json, sys
from pyspark.sql import SparkSession, Row

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("ImdbVeniceWinnersJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

# year = '2022'
# festa_name = 'venice'
# Airflow 에서 받을 파라미터
year = sys.argv[1]
festa_name = sys.argv[2]

venice_path = make_imdb_file_dir(festa_name, year)
venice_data = get_s3_data(venice_path)

# 1961~1968 & 1980~2015 Golden Lion
def process_imdb_venice1(json_str):
    json_obj = json.loads(json_str)
    final_data = []
    venice_awards_list = json_obj["nomineesWidgetModel"]["eventEditionSummary"]["awards"]
    awards_winner_list_raw = []

    for i in venice_awards_list:
        awards_winner_list_raw.append(i)

    for i in awards_winner_list_raw:
        # print(i["awardName"])
        awardName = i["awardName"]
        awardCategory = []
        awardWinner = []
        awardImages = []

        for cate in i["categories"]:
            awardCategory.append(cate["categoryName"])
            for item in i["categories"][0]["nominations"]:
                if item["isWinner"] is False:
                    pass
                else:
                    awardWinner.append(item["primaryNominees"][0]["name"])
                    awardImages.append(item["primaryNominees"][0]["imageUrl"])

        period_tuple = (awardName, awardCategory, awardWinner, awardImages)
        final_data.append(period_tuple)

    return final_data
    # print(final_data)

# 데이터를 RDD로 변환
raw_imdb_rdd = spark.sparkContext.parallelize([venice_data])
transformed_rdd = raw_imdb_rdd.map(process_imdb_venice1)
tmp_rdd = transformed_rdd.collect()[0]
rdd_rows = [Row(award_name=row[0], award_category=row[1],award_winner=row[2], award_image=row[3]) for row in tmp_rdd]

venice_df = spark.createDataFrame(rdd_rows)
# s3 저장 경로 
parquet_path = f's3a://sms-warehouse/imdb/{festa_name}/{year}'
# imdb_academy_1931
filename = f'imdb_{festa_name}_{year}'
venice_df.write.parquet(f'{parquet_path}/{filename}')

spark.stop()
