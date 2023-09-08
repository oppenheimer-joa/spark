from pyspark.sql import SparkSession
import json
import sys
from modules import *

# Spark Session Build
access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("KOPIS to DF") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
    .getOrCreate()

print("spark session built successfully")

rdd_list = []
date = sys.argv[1]
year = date[:4]

def get_spotify_data(year, movie_id):

#    s3_path = f's3a://sms-basket/spotify/{year}/*{movie_id}*.json'
#    data = spark.read.json(s3_path)

    try:
        s3_path = f's3a://sms-basket/spotify/{year}/*{movie_id}*.json'
        file_list = spark.sparkContext.wholeTextFiles(s3_path)
        row = file_list.collect()[0]
        json_string=row[1]
        return movie_id, json_string
    
    except Exception as e:
        print(s3_path, "<<<<< Not found >>>>>")
        print(str(e))
        return movie_id, ""


def get_movie_id(date) :  # date = YYYY-MM-DD

    file_list = spark.sparkContext.wholeTextFiles(f"s3a://sms-basket/TMDB/detail/{date}/*.json")
    movie_id_list = [row[0].split("_")[2] for row in file_list.collect()]

    #return movie_id_list
    return movie_id_list


def transform_spotify_json(movie_id, json_data) :

    json_data = json.loads(json_data)
    data = json_data['albums']['items']
    musics = []

    for item in data :
        name = item['name']
        artist = item['artists'][0]['name']
        url = item['external_urls']['spotify']
        image = item['images'][0]['url']
        musics.append([name, artist, url, image])

    rdd_list.append({'movie_id': movie_id, 'album': musics})


movie_id_list = get_movie_id(date)

for movie in movie_id_list :
    print(movie)
    movie_id, raw_data = get_spotify_data(year=year, movie_id=movie)
    if raw_data != "" :
        transform_spotify_json(movie_id, raw_data)
    else :
         rdd_list.append({'movie_id': movie_id, 'album': []})

raw_rdd = spark.sparkContext.parallelize([rdd_list])

json_df = spark.read.json(raw_rdd)
json_df.show()

