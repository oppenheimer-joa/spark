import findspark
findspark.init()

import pyspark
findspark.find()

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
conf.set("parquet.enable.summary-metadata", "false")
conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
conf.set("spark.sql.parquet.writeLegacyFormat", "true")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

import tmp_lib
import json

date='2023-08-07'
file_list = tmp_lib.get_raw_data(date)
# print(len(file_list))

def transform_json(json_str):
    data = json.loads(json_str)

    try :
        tksite_temp = data.get('tksites').get('tksite')

        if type(tksite_temp) == type([]):
            tksite_pro=[{tksite['#text']:tksite['@href']} for tksite in tksite_temp]
        else:
            tksite_pro=[{tksite_temp['#text']:tksite_temp['@href']}]

    except:
        tksite_pro=[]

    try:
        styurl = list(data.get('styurls').values())

        if type(styurl[0]) == type([]) :
            styurl=sum(styurl,[])

    except :
        styurl=[]

    data['styurls'] = str(styurl)
    data['tksites'] = str(tksite_pro)

    return json.dumps(data)

# 데이터를 RDD로 변환
raw_image_rdd = spark.sparkContext.parallelize(file_list)
transformed_image_rdd = raw_image_rdd.map(transform_json)

# 스키마 정의
schema = StructType([
    StructField("mt20id", StringType(), True),
    StructField("prfnm", StringType(), True),
    StructField("prfpdfrom", StringType(), True),
    StructField("prfpdto", StringType(), True),
    StructField("fcltynm", StringType(), True),
    StructField("prfcast", StringType(), True),
    StructField("prfcrew", StringType(), True),
    StructField("prfruntime", StringType(), True),
    StructField("prfage", StringType(), True),
    StructField("entrpsnm", StringType(), True),
    StructField("pcseguidance", StringType(), True),
    StructField("poster", StringType(), True),
    StructField("sty", StringType(), True),
    StructField("genrenm", StringType(), True),
    StructField("prfstate", StringType(), True),
    StructField("openrun", StringType(), True),
    StructField("styurls", StringType(), True),
    StructField("mt10id", StringType(), True),
    StructField("dtguidance", StringType(), True),
    StructField("tksites", StringType(), True)
])

# 변환된 JSON 데이터 출력
json_df = spark.read.schema(schema).json(transformed_image_rdd)
# 데이터프레임 보기
json_df.show(truncate=3)

# Parquet 파일로 저장
output_path = f"file:///home/sub/cong/spark/data/KOPIS_{date}.parquet"
json_df = json_df.coalesce(1)
json_df.write.parquet(output_path)

spark.stop()