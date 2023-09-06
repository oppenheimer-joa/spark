from lib.modules import *
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("JsonToDetailRdd").getOrCreate()

year = '1960-01-01'
movie_code = '1000336'
category = 'detail'

detail_path = make_tmdb_file_dir(category, year, movie_code)
detail_data = get_TMDB_data(detail_path)

raw_detail_rdd = spark.sparkContext.parallelize([detail_data])

#detail 전처리 함수
def transform_TMDB_detail_json(json_data):
    try:
        data = json.loads(json_data)
        keys_to_remove = ["vote_count", "vote_average", "title", "tagline", "status", "popularity","adult"]
        for key in keys_to_remove:
            if key in data:
                del data[key]

        genres = data.get("genres", [])
        genre_ids = [genre["id"] for genre in genres]
        data["genres"] = genre_ids

        return data
    except json.JSONDecodeError as e:
        return (f"json decode err : {e}")

transformed_detail_rdd = raw_detail_rdd.map(transform_TMDB_detail_json)

#API 서버내에 데이터 rdd 데이터 transformed_image_rdd 저장
transformed_detail_rdd.saveAsTextFile(f"/Users/jesse/Documents/sms/spark/detail_{year}_{movie_code}")