import subprocess
import mysql.connector
import configparser
from datetime import datetime, timedelta
import threading

def get_config(group, req_var):
    config = configparser.ConfigParser()
    config.read("/home/spark/spark_code/config/config.ini")
    result = config.get(group, req_var)
    return result

def run_tmdb_command(date_gte, script_path):
    command = ["sh", "/home/spark/spark_code/sh/tmdb_pyspark.sh", script_path, date_gte]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")

start_date = '2000-01-07'
start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

# 병렬로 실행할 명령 스크립트 경로
script_paths = [
    "/home/spark/spark_code/src/Tmdb_transform_details.py",
    "/home/spark/spark_code/src/Tmdb_transform_credit.py",
    "/home/spark/spark_code/src/Tmdb_transform_images.py",
    "/home/spark/spark_code/src/Tmdb_transform_similar.py"
]

# 스레드 수
num_threads = 8

while start_datetime.year < 2001:

    date_gte_list =[]

    for i in range(1,9):
        if i % 4 == 0:
            date_gte_list.append(start_datetime.strftime('%Y-%m-%d'))
            start_datetime += timedelta(days=7)
        else:
            date_gte_list.append(start_datetime.strftime('%Y-%m-%d'))

    threads = []

    for i, script_path in enumerate(script_paths):
        date_gte = date_gte_list[i]
        thread = threading.Thread(target=run_tmdb_command, args=(date_gte, script_path))
        threads.append(thread)
        if len(threads) >= num_threads:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            threads = []


