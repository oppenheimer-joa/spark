from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, explode, lit
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType
import json

spark = SparkSession.builder.appName("JsonToDataFrame").getOrCreate()

# api query endpoint로 부터 전달받아야할 것 year, movie_code
year = '1960-01-01'
movie_code = '1000336'

# TMDB 에 받아야할 category list
categories = ['detail', 'credit', 'image', 'similar']

for category in categories:
    file_data = f'{category}_data'

    locals()[file_data] = make_tmdb_file_dir(category, year, movie_code)
    locals()[file_data] = get_TMDB_data(locals()[file_data])

# 이미지 데이터를 RDD로 변환
raw_image_rdd = spark.sparkContext.parallelize([image_data])
raw_detail_rdd = spark.sparkContext.parallelize([detail_data])
raw_similar_rdd = spark.sparkContext.parallelize([similar_data])
raw_credit_rdd = spark.sparkContext.parallelize([credit_data])

# JSON 데이터 변환 함수
def transform_TMDB_image_json(json_data):
    try:
        data = json.loads(json_data)
        posters = data.get("posters", [])
        if posters:
            poster_file_path = posters[0].get("file_path", "")
            data["posters"] = poster_file_path
            return json.dumps(data)
        return json.dumps(data)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return json.dumps(data)

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

        return json.dumps(data)
    except json.JSONDecodeError as e:
        print(f"json decode err : {e}")
        return json.dumps(data)

def transform_TMDB_similar_json(json_data):
    try:
        data = json.loads(json_data)
        keys_to_remove = ["total_pages", "total_results", "page"]
        
        # key 삭제
        for key in keys_to_remove:
            if key in data:
                del data[key]

        results = data.get("results", [])
        similar_ids = []
        for i in range(5):
            similar_ids.append(results[i]["id"])
        data["results"] = similar_ids
        data["id"] = int(movie_code)
        return json.dumps(data)

    except json.JSONDecodeError as e:
        print(f"json decode err : {e}")
        return json.dumps(data)

def transform_TMDB_credit_json(json_data):
    try:
        data = json.loads(json_data)

        crews = data.get("crew", [])
        crew_ids = []

        if len(crews) == 0:
            pass

        else:
            for i in range(len(crews)):
                crew_ids.append(crews[i]["id"])
            data["crew"] = crew_ids

        casts = data.get("cast", [])
        cast_ids = []

        if len(casts) == 0:
            pass

        else:
            for j in range(len(casts)):
                cast_ids.append(casts[j]["id"])
            data["cast"] = cast_ids

        return data
    except JSONDecodeError as e:
        return json.dumps(data)

# image json 변환
transformed_image_rdd = raw_image_rdd.map(transform_TMDB_image_json)
#transformed_image_rdd.foreach(print)

# detail json 변환
transformed_detail_rdd = raw_detail_rdd.map(transform_TMDB_detail_json)
#transformed_detail_rdd.foreach(print)

# similar json 변환
transformed_similar_rdd = raw_similar_rdd.map(transform_TMDB_similar_json)
#transformed_similar_rdd.foreach(print)

# credit json 변환
transformed_credit_rdd = raw_credit_rdd.map(transform_TMDB_credit_json)
#transformed_credit_rdd.foreach(print)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("cast", ArrayType(IntegerType(), True), True),
    StructField("crew", ArrayType(IntegerType(), True), True)
])

# DataFrame을 생성합니다.
credit_df = spark.createDataFrame(transformed_credit_rdd, schema=schema)

# DataFrame을 출력합니다.
credit_df.show()




















