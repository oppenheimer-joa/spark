import json, time
from lib.modules import *
import pandas as pd
from pyspark.sql import SparkSession, Row

start_time = time.process_time()

def s3_file_to_json(file_path):

    s3 = create_s3client()

    s3_object = s3.get_object(Bucket='sms-basket', Key=file_path)

    raw_data = s3_object['Body'].read()
    json_str = raw_data.decode('utf-8')

    return json_str


def process_imdb_venice1(json_str): #1961~1968 & 1980~2015
    json_obj = json.loads(json_str)
    final_data = {"award_name":[], "award_category":[],"award_winner":[],"award_image":[]}
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

        final_data["award_name"].append(awardName)
        final_data["award_category"].append(awardCategory)
        final_data["award_winner"].append(awardWinner)
        final_data["award_image"].append(awardImages)

    return final_data
    # print(final_data)

year = "1968"
imdb_data = s3_file_to_json(f"IMDb/imdb_venice_{year}.json")
data = process_imdb_venice1(imdb_data)

# 딕셔너리를 DataFrame으로 변환합니다.
df = pd.DataFrame(data)
print(df)
csv_file_path = f"/Users/woorek/Downloads/rdd/venice_{year}.csv"
df.to_csv(csv_file_path, header=True, index=False)

end_time = time.process_time()
elapsed_time = end_time - start_time
print(f"프로세스 시간: {elapsed_time} 초")

