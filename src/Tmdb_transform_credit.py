from lib.modules import *
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("JsonToCreditRdd").getOrCreate()

year = '1960-01-01'
movie_code = '1000336'
category = 'credit'

credit_path = make_tmdb_file_dir(category, year, movie_code)
credit_data = get_TMDB_data(credit_path)

raw_credit_rdd = spark.sparkContext.parallelize([credit_data])

#credit 전처리 함수
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


transformed_credit_rdd = raw_credit_rdd.map(transform_TMDB_credit_json)

#API 서버내에 데이터 rdd 데이터 transformed__rdd 저장
transformed_credit_rdd.saveAsTextFile(f"/Users/jesse/Documents/sms/spark/credit_{year}_{movie_code}")
