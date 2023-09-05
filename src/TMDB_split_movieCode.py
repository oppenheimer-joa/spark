from lib.modules import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, explode,lit


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

join_img_df.show()
join_detail_df.show()




