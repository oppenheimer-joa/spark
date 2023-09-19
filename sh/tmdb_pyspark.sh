#!/bin/bash

# 실행 커맨드 = sh pyspark.sh $SPARK_FILE $SPARK_HOME
# #SPARK_FILE은 파일의 절대 경로 입력

# SPARK FILE 가져오기
SPARK_HOME='/home/spark/spark/spark-3.2.4'
SPARK_FILE="$1"
DATE="$2"
#MOVIE_CODE="$3"

# Spark Job 제출
# 각자의 spark 환경에 맞게 설정 필요
# : 절대경로, master 링크, executor 메모리 및 코어
$SPARK_HOME/bin/spark-submit --master spark://master:7077 --total-executor-cores=2 $SPARK_FILE $DATE #$MOVIE_CODE