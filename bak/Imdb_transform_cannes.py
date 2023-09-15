from lib.modules import *
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import json

sc = SparkContext(appName="transformCannesFilm")
spark = SparkSession.builder.appName("CannesWinners").getOrCreate()


year = '2022'
festa_name = 'cannes'

cannes_path = make_imdb_file_dir(festa_name, year)
cannes_data = get_s3_data(cannes_path)

raw_cannes_rdd = spark.sparkContext.parallelize([cannes_data])
# data = {"Honorary Golden Palm": "Forest Whitaker", "Golden Camera": "War Pony"}
def transform_cannes_data(json_data):
    try:
        final_data =[]

        data = json.loads(json_data)["nomineesWidgetModel"]
        keys_to_remove = ["title", "description", "refMarker", "alwaysDisplayAwardNames", "shouldDisplayNotes", "showOnlyPremiumCategories", "mobile"]

        # json 가장 바깥쪽 지우기
        for key in keys_to_remove:
            del data[key]

        # json layers 안 데이터 삭제
        keys_to_remove = ["eventEditionId", "eventId", "occurrence", "requestKey", "eventName", "year"]
        for key in keys_to_remove:
            del data['eventEditionSummary'][key]

        data = data["eventEditionSummary"]["awards"]

        for i in range(len(data)):
            keys_to_remove = ["trivia", "id"]
            for key in keys_to_remove:
                del data[i][key]

            award_name = data[i]["awardName"]
            movie_name = data[i]["categories"][0]["nominations"][0]["primaryNominees"][0]["name"]
            winner_image = data[i]["categories"][0]["nominations"][0]["primaryNominees"][0]["imageUrl"]
            tmp_tuple = (award_name, movie_name, winner_image)
            final_data.append(tmp_tuple)
        return final_data

    except json.JSONDecodeError as e:
        return f"json decode err : {e}"

#데이터를 가공 후, rdd list반환 후, 한겹 벗겨내기
transformed_cannes_data = raw_cannes_rdd.map(transform_cannes_data).collect()[0]

rdd_rows = [Row(price_name=row[0], winner=row[1], winner_image=row[2]) for row in transformed_cannes_data]

#row로 만들어진 rdd를 df로 생성
cannes_df = spark.createDataFrame(rdd_rows)

#df 저장하는 작업만 남았을뿐.
cannes_df.show()



