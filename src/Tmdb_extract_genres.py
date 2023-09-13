import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("ParquetsToExtractMovieGenres") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

date = sys.argv[1]
movie_code = sys.argv[2]

base_dir = f"s3a://sms-warehouse/TMDB/{date}/TMDB_movie_{movie_code}_{date}"
base_df = spark.read.parquet(base_dir)
genre_df = base_df.select("genres")

desired_genres = {
    "Action": 28,
    "Adventure": 12,
    "Animation": 16,
    "Comedy": 35,
    "Crime": 80,
    "Documentary": 99,
    "Drama" : 18,
    "Family" : 10751,
    "Fantasy" : 14,
    "History" : 36,
    "Horror" : 27,
    "Music" : 10402,
    "Mystery" : 9648,
    "Romance" : 10749,
    "Science_Fiction" : 878,
    "TV_Movie" : 10770,
    "Thriller" : 53,
    "War" : 10752,
    "Western" : 37
}

for genre, genre_code in desired_genres.items():
    genre_df = genre_df.withColumn(genre, when(col("genres").cast("string").contains(str(genre_code)), 1).otherwise(0))

genre_df = genre_df.withColumn("id", lit(movie_code))
genre_df = genre_df.drop("genres")

genre_df.show() 

s3_path = f's3a://sms-warehouse/genre/{date}'
filename = f'TMDB_movie_{movie_code}_{date}'
genre_df.write.mode("overwrite").parquet(f'{s3_path}/{filename}')



