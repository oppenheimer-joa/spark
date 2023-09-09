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


def transform_academy(json_str):
    json_obj = json.loads(json_str)
    venice_awards_list = json_obj["nomineesWidgetModel"]["eventEditionSummary"]["awards"]

    final_data = []

    for i in venice_awards_list:
        award_Name = i["awardName"]
        for cate in i["categories"]:
            for nomi in cate["nominations"]:
                if nomi["isWinner"] is True:
                    award_Category = nomi["categoryName"]

                    if len(nomi["primaryNominees"]) != 0:
                        award_Winner = nomi["primaryNominees"][0]["name"]
                        award_Image = nomi["primaryNominees"][0]["imageUrl"]
                    else:
                        pass

                    period_tuple = (award_Name, award_Category, award_Winner, award_Image)
                    final_data.append(period_tuple)
                else:
                    pass

    return final_data

year = "2005"
festa = "academy"
imdb_data = s3_file_to_json(f"IMDb/imdb_{festa}_{year}.json")

# 데이터를 RDD로 변환
raw_imdb_rdd = spark.sparkContext.parallelize([imdb_data])
transformed_rdd = raw_imdb_rdd.map(transform_academy)
tmp_rdd = transformed_rdd.collect()[0]
rdd_rows = [Row(award_name=row[0], award_category=row[1],award_winner=row[2], award_image=row[3]) for row in tmp_rdd]

df = spark.createDataFrame(rdd_rows)
df.show()

