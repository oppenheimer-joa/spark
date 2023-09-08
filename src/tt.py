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

cannes_df.show()
