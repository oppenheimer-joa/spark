import findspark
findspark.init()

# from lib.modules import *
from lib.modules import *

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
import json, os
from pyspark import SparkContext, SparkConf

# spark = SparkSession.builder.appName("VenicetoRdd").getOrCreate()

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

imdb_data = s3_file_to_json("IMDb/imdb_venice_1968.json")
fn_data = process_imdb_venice1(imdb_data)
print(fn_data)
print(type(fn_data))


# def process_imdb_venice1(json_str):
#     try:
#         json_obj = json.loads(json_str)
#         venice_awards_list = json_obj["nomineesWidgetModel"]["eventEditionSummary"]["awards"]
#         tmp_nominee_list = []
#         for i in venice_awards_list:
#             if i["awardName"] == "Golden Lion":
#                 tmp_nominee_list.append(i["categories"][0]["nominations"])
#             else:
#                 pass
#
#         nominee_list = tmp_nominee_list[0]
#
#         for i in nominee_list:
#             if i["isWinner"] is True:
#                 return i["primaryNominees"][0]["originalName"]
#         return None
#     except:
#         return None
#
#
# spark = SparkSession.builder.appName("IMDBVeniceProcessing").getOrCreate()
#
# udf_process_imdb_venice1 = udf(process_imdb_venice1, StringType())
#
#
# json_data_list = []
# years = ["1961","1962","1963","1964","1965","1966","1967","1968"]
# for i in years:
#     file_path = f"IMDb/imdb_venice_{i}.json"
#     json_data_list.append(s3_file_to_json(file_path))
#
# schema = StructType([StructField("json_data", StringType(), True)])
# json_df = spark.createDataFrame(json_data_list, schema)
# result_df = json_df.withColumn("movie_nm", udf_process_imdb_venice1(json_df["json_data"]))
# # 결과를 저장하거나 출력
# result_df.show()
#
# # SparkSession 종료
# spark.stop()



