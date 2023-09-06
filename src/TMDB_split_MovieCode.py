from modules import *

import findspark
findspark.init()

import pyspark
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, explode,lit

conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
conf.set("parquet.enable.summary-metadata", "false")
conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
conf.set("spark.sql.parquet.writeLegacyFormat", "true")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

# api query endpoint로 부터 전달받아야할 것 year, movie_code
year = '1960-01-01'
movie_code = '1000336'

# TMDB 에 받아야할 category list
categories = ['detail', 'credit', 'image', 'similar']

for category in categories:
    file_data = f'{category}_data'

    locals()[file_data] = make_tmdb_file_dir(category, year, movie_code)
    locals()[file_data] = get_TMDB_data(locals()[file_data])


# 병합 집계낼 json spark df 변환
raw_image = spark.read.json(spark.sparkContext.parallelize([image_data])).drop('backdrops')
raw_detail = spark.read.json(spark.sparkContext.parallelize([detail_data]))
raw_similar = spark.read.json(spark.sparkContext.parallelize([similar_data]))
raw_credit = spark.read.json(spark.sparkContext.parallelize([credit_data]))

# json explode
exploded_image_df = raw_image.select("id", "logos", explode("posters").alias("poster"))
exploded_similar_df = raw_similar.select("page",explode("results").alias("results"),"total_pages", "total_results")
exploded_credit_df = raw_credit.select("id", explode("cast").alias("cast"), "crew")

tmp_similar_movieCode = exploded_similar_df.select("results.id").limit(5)

# 최종 join할 img_df
join_img_df = exploded_image_df.select("id", "logos", "poster.aspect_ratio", "poster.height","poster.file_path")

# 최종 join할 detail_df
join_detail_df = raw_detail.drop('adult','popularity', 'status', 'tagline', 'title','vote_average', 'vote_count')

join_img_df.show(truncate=3)
join_detail_df.show(truncate=3)

# id를 기준으로 outer join을 수행합니다.
result_df = join_img_df.join(join_detail_df, join_img_df["id"] == join_detail_df["id"], "outer")

# 결과를 보여줍니다.
result_df.show()

# rdd1 = sc.parallelize([join_img_df])
# rdd2 = sc.parallelize([join_detail_df])
# print(sorted(rdd1.fullOuterJoin(rdd2).collect()))


# print(sorted(join_img_df.fullOuterJoin(join_detail_df).collect()))