from lib.modules import *
import json, sys, time
from pyspark.sql import SparkSession, Row


start_time = time.process_time()

# Spark session 초기화
spark = SparkSession.builder \
    .appName("imdb venice") \
    .master("local[1]") \
    .getOrCreate()

def s3_file_to_json(file_path):

    s3 = create_s3client()

    s3_object = s3.get_object(Bucket='sms-basket', Key=file_path)

    raw_data = s3_object['Body'].read()
    json_str = raw_data.decode('utf-8')

    return json_str

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



year = "2022"
imdb_data = s3_file_to_json(f"IMDb/imdb_venice_{year}.json")

# 데이터를 RDD로 변환
raw_imdb_rdd = spark.sparkContext.parallelize([imdb_data])
transformed_rdd = raw_imdb_rdd.map(process_imdb_venice1)
tmp_rdd = transformed_rdd.collect()[0]
rdd_rows = [Row(award_name=row[0], award_category=row[1],award_winner=row[2], award_image=row[3]) for row in tmp_rdd]

df = spark.createDataFrame(rdd_rows)
df.show()
