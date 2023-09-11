from pyspark.sql import SparkSession
from pyspark import SparkContext
import json

sc = SparkContext(appName="LoadTempFiles")
spark = SparkSession(sc)

year = '1960-01-01'
movie_code = '1000336'
categories = ['image','detail', 'credit', 'similar']

# 폴더 패스 들어오면 해당 rdd를 받아 반환
def load_tmp_tmdb_data(folder_path):
	tmp_rdd = sc.textFile(folder_path)
	return tmp_rdd.collect()

# rdd -> df
def convert_df_temp_data(json_data):
	print(json_data)
	print(type(json_data))
	df = spark.read.json(sc.parallelize(json_data))
	return df

# 동적 할당한 변수에 읽어온 rdd 할당
for category in categories:
	rdd_var = f"{category}_rdd"
	folder_path = f"/Users/jesse/Documents/sms/spark/{category}_{year}_{movie_code}"
	rdd_data = load_tmp_tmdb_data(folder_path)

	# DataFrame 생성 및 전역 변수에 할당
	df_var = f"{category}_df"
	locals()[df_var] = convert_df_temp_data(rdd_data)

# 동적할당된 변수 다 불러다가 join

result_df = credit_df.join(image_df, on='id', how='inner')\
    .join(detail_df, on='id', how='inner')\
    .join(similar_df, on='id', how='inner')

result_df.show()

'''
image_df.show()
similar_df.show()
detail_df.show()
credit_df.show()
'''









