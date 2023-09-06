from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType
import json

spark = SparkSession.builder.appName("JsonToImageRdd").getOrCreate()

year = '1960-01-01'
movie_code = '1000336'
categories = 'image'

image_path = make_tmdb_file_dir(category, year, movie_code)
image_data = get_TMDB_data(image_path)

raw_image_rdd = spark.sparkContext.parallelize([image_data])

#image 전처리 함수
def transform_TMDB_image_json(json_data):
    try:
        data = json.loads(json_data)
        posters = data.get("posters", [])
        if posters:
            poster_file_path = posters[0].get("file_path", "")
            data["posters"] = poster_file_path
            
        return data

    except json.JSONDecodeError as e:
        return f"JSON decoding error: {e}"

transformed_image_rdd = raw_image_rdd.map(transform_TMDB_image_json)

#API 서버내에 데이터 rdd 데이터 transformed_image_rdd 저장

transformed_image_rdd.saveAsTextFile("file_path")