import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
# from pyspark import SparkContext
from pyspark.sql import SparkSession, Row


# sc = SparkContext(appName="transformCannesFilm")

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("ImdbCannesWinnersJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

# year = '2022'
# Airflow 에서 받을 파라미터
year = sys.argv[1]
festa_name = 'cannes'

cannes_path = make_imdb_file_dir(festa_name, year)
cannes_data = get_s3_data(cannes_path)

def transform_cannes_data(json_data):
    try:
        data = json.loads(json_data)["nomineesWidgetModel"]
        event_summary = data.get("eventEditionSummary", {})
        awards = event_summary.get("awards", [])

        result = []
        for award in awards:
            award_name = award.get("awardName", "")
            nominations = award.get("categories", [])[0].get("nominations", [])
            if nominations:
                nominee = nominations[0].get("primaryNominees", [])[0]
                movie_name = nominee.get("name", "")
                winner_image = nominee.get("imageUrl", "")
                result.append((award_name, movie_name, winner_image))

        return result

    except json.JSONDecodeError as e:
        return f"json decode err : {e}"


raw_cannes_rdd = spark.sparkContext.parallelize([cannes_data])
transformed_cannes_data = raw_cannes_rdd.flatMap(transform_cannes_data)

cannes_data_list = transformed_cannes_data.collect()

rdd_rows = [Row(award_name=row[0], winner=row[1], winner_image=row[2]) for row in cannes_data_list]
cannes_df = spark.createDataFrame(rdd_rows)
# s3 저장 경로 
parquet_path = f's3a://sms-warehouse/imdb/{festa_name}/{year}'
# imdb_academy_1931
filename = f'imdb_{festa_name}_{year}'
cannes_df.write.parquet(f'{parquet_path}/{filename}')

spark.stop()
