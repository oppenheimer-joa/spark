import findspark
findspark.init()

# from lib.modules import *
from lib.modules import *

from pyspark.sql import SparkSession, Row
import json, os
import pandas as pd
from pyspark import SparkContext, SparkConf


def s3_file_to_json(file_path):

    s3 = create_s3client()

    s3_object = s3.get_object(Bucket='sms-basket', Key=file_path)

    raw_data = s3_object['Body'].read()
    json_str = raw_data.decode('utf-8')

    return json_str

def process_imdb_venice1(json_str): #1961~1968 & 1980~2015
    json_obj = json.loads(json_str)
    venice_awards_list = json_obj["nomineesWidgetModel"]["eventEditionSummary"]["awards"]
    tmp_nominee_list = []
    for i in venice_awards_list:
        if i["awardName"] == "Golden Lion":
            tmp_nominee_list.append(i["categories"][0]["nominations"])
        else:
            pass

    nominee_list = tmp_nominee_list[0]

    for i in nominee_list:
        if i["isWinner"] is True:
            data = {"movie_nm": i["primaryNominees"][0]["originalName"]}
            return data
        else:
            return "1"






# Spark session 초기화
# spark = SparkSession.builder.appName("JSON to DF").getOrCreate()

# conf = SparkConf().setAppName("Complex JSON Processing")
# sc = SparkContext(conf=conf)


imdb_data = s3_file_to_json("IMDb/imdb_venice_1968.json")
fn_data = process_imdb_venice1(imdb_data)
print(fn_data)
print(type(fn_data))
# rdd = sc.parallelize(json_data)
