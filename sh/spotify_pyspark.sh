#!/bin/bash

# SPARK FILE 가져오기
SPARK_HOME='/home/ubuntu/spark/spark-3.2.4'
date="$1"

# Spark Job 제출
# 각자의 spark 환경에 맞게 설정 필요
# : 절대경로, master 링크, executor 메모리 및 코어
$SPARK_HOME/bin/spark-submit --master spark://master:7077 src/Spotify_transform_data.py $date