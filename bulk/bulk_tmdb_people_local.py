import subprocess, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
import mysql.connector
from datetime import datetime, timedelta
import threading


def run_tmdb_people_command(date_gte):
    command = ["sh", "/home/spark/spark_code/sh/tmdb_people_pyspark.sh", date_gte]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")

start_date = '2023-01-06'
start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

# 스레드 수
num_threads = 8

while start_datetime < datetime(2023,9,18):

    date_gte_list =[]

    for i in range(num_threads):
        date_gte_list.append(start_datetime.strftime('%Y-%m-%d'))
        start_datetime += timedelta(days=7)

    threads = []

    for i in range(num_threads):
        date_gte = date_gte_list[i]
        thread = threading.Thread(target=run_tmdb_people_command, args=(date_gte,))
        threads.append(thread)
        if len(threads) >= num_threads:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            threads = []

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()