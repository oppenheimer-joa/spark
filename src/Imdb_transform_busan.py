from lib.modules import *
from pyspark.sql import SparkSession, Row
import json

spark = SparkSession.builder.appName("CannesWinners").getOrCreate()

year = '2022'
festa_name = 'busan'

busan_path = make_imdb_file_dir(festa_name, year)
busan_data = get_s3_data(busan_path)

def del_nominee_data(json_data):
    try:
        data = json.loads(json_data)["nomineesWidgetModel"]
        keys_to_delete = ["title", "description", "refMarker", "alwaysDisplayAwardNames", "shouldDisplayNotes", "showOnlyPremiumCategories", "mobile"]
        for key in keys_to_delete:
            del data[key]
        return json.dumps(data)
    except json.JSONDecodeError as e:
        return f"json decode err : {e}"

def get_awards_data(json_data):
    try:
        data = json.loads(json_data)
        keys_to_remove = ["eventEditionId", "eventId", "occurrence", "requestKey", "eventName", "year"]
        for key in keys_to_remove:
            del data['eventEditionSummary'][key]

        data = data["eventEditionSummary"]["awards"]
        return json.dumps(data)
    except json.JSONDecodeError as e:
        return f"json decode err : {e}"

def get_final_data(json_data):
    try:
        final_data =[]
        data = json.loads(json_data)
        for i in range(len(data)):
            award_host = data[i]["awardName"]
            award_name = data[i]["categories"][0]["categoryName"]
            award_winner = data[i]["categories"][0]["nominations"][0]["primaryNominees"][0]["name"]
            award_image = data[i]["categories"][0]["nominations"][0]["primaryNominees"][0]["imageUrl"]
            winner_tuple = (award_host,award_name, award_winner, award_image)
            final_data.append(winner_tuple)
        return final_data
    except json.JSONDecodeError as e:
        return f"json decode err : {e}"

raw_busan_rdd = spark.sparkContext.parallelize([busan_data])
del_base_rdd = raw_busan_rdd.map(del_nominee_data)
award_base_rdd = del_base_rdd.map(get_awards_data)
final_rdd = award_base_rdd.map(get_final_data).collect()[0]

rdd_rows = [Row(award_host=row[0], award_name=row[1], winner=row[2], award_img=row[3]) for row in final_rdd]

#row로 만들어진 rdd를 df로 생성
busan_df = spark.createDataFrame(rdd_rows)
busan_df.show()
