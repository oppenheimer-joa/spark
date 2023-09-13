#!/bin/bash

# 실행 커맨드 = sh pyspark.sh $SPARK_FILE $SPARK_HOME
# #SPARK_FILE은 파일의 절대 경로 입력

# SPARK FILE 가져오기
SPARK_HOME='/home/ubuntu/spark/spark-3.2.4'

# Spark Job 제출
# 각자의 spark 환경에 맞게 설정 필요
# : 절대경로, master 링크, executor 메모리 및 코어
$SPARK_HOME/bin/spark-submit --master spark://master:7077 src/Imdb_join_parquet.py