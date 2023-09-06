from pyspark import SparkContext
from pyspark.sql import SparkSession

# SparkContext 초기화
sc = SparkContext(appName="TextFileExample")

# SparkSession 초기화
spark = SparkSession(sc)

# 저장된 폴더 경로 설정
folder_path = "/Users/jesse/Documents/sms/spark/1960-01-01_1000336"

# 폴더로부터 RDD를 불러오기
rdd = sc.textFile(folder_path)

# RDD를 출력
rdd.foreach(print)

# SparkContext 종료
sc.stop()
