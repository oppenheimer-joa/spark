from lib.modules import *
import json
from pyspark.sql import SparkSession, Row


# Spark session 초기화
spark = SparkSession.builder \
    .appName("imdb academy") \
    .master("local[1]") \
    .getOrCreate()

def s3_file_to_json(file_path):

    s3 = create_s3client()

    s3_object = s3.get_object(Bucket='sms-basket', Key=file_path)

    raw_data = s3_object['Body'].read()
    json_str = raw_data.decode('utf-8')

    return json_str


year = "2022"
festa = "academy"
imdb_data = s3_file_to_json(f"IMDb/imdb_{festa}_{year}.json")

# 데이터를 RDD로 변환
raw_imdb_rdd = spark.sparkContext.parallelize([imdb_data])
transformed_rdd = raw_imdb_rdd.map(transform_imdb)
tmp_rdd = transformed_rdd.collect()[0]
rdd_rows = [Row(award_name=row[0], award_category=row[1],award_winner=row[2], award_image=row[3]) for row in tmp_rdd]

df = spark.createDataFrame(rdd_rows)
df.show()

