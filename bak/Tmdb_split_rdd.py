from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType
import json

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("TmdbJsonToDataFrame") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()
    
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

# image rdd 변환 및 전처리 함수
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

# detail rdd 변환 및 전처리 함수
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

# similar rdd 변환 및 전처리 함수
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
        return data

    except json.JSONDecodeError as e:
        return f"json decode err : {e}"

# credit rdd 변환 및 전처리 함수
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
    except json.JSONDecodeError as e:
        return f"wrong code :  {e}"

# 전처리 DATA rdd 반환
transformed_image_rdd = raw_image_rdd.map(transform_TMDB_image_json)
transformed_detail_rdd = raw_detail_rdd.map(transform_TMDB_detail_json)
transformed_similar_rdd = raw_similar_rdd.map(transform_TMDB_similar_json)
transformed_credit_rdd = raw_credit_rdd.map(transform_TMDB_credit_json)


#credit df 변환
credit_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("cast", ArrayType(StringType(), True), True),
    StructField("crew", ArrayType(StringType(), True), True)
])

#image df 변환
image_schema = StructType([
    StructField("backdrops", ArrayType(StringType(), True), True),
    StructField("id", IntegerType(), True),
    StructField("logos", ArrayType(StringType(), True), True),
    StructField("posters", StringType(), True)
])

#similar df 변환
similar_schema = StructType([
    StructField("results", ArrayType(IntegerType(), True), True),
    StructField("id", IntegerType(), True)
])

#detail df 변환
detail_schema = StructType([
    StructField("backdrop_path", StringType(), True),
    StructField("belongs_to_collection", StringType(), True),
    StructField("budget", IntegerType(), True),
    StructField("genres", ArrayType(IntegerType(), True), True),
    StructField("homepage", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("logo_path", StringType(), True),
        StructField("name", StringType(), True),
        StructField("origin_country", StringType(), True)
    ]), True), True),
    StructField("production_countries", ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ]), True), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("spoken_languages", ArrayType(StructType([
        StructField("iso_639_1", StringType(), True),
        StructField("name", StringType(), True)
    ]), True), True),
    StructField("video", StringType(), True)
])

# 전처리 RDD -> Dataframe 변환
similar_df = spark.createDataFrame(transformed_similar_rdd, schema=similar_schema)
credit_df = spark.createDataFrame(transformed_credit_rdd, schema=credit_schema)
detail_df = spark.createDataFrame(transformed_detail_rdd, schema=detail_schema)
image_df = spark.createDataFrame(transformed_image_rdd, schema=image_schema)

# dataframe join
result_df = credit_df.join(image_df, on='id', how='inner')\
    .join(detail_df, on='id', how='inner')\
    .join(similar_df, on='id', how='inner')


#로컬 parquet 다운 테스트
#result_df.write.parquet(f"/Users/jesse/Documents/sms/spark/{year}_{movie_code}.parquet")


























