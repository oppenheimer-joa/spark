import subprocess
import mysql.connector
import configparser
from datetime import datetime, timedelta
import threading

def get_config(group, req_var):
    config = configparser.ConfigParser()
    config.read("/home/spark/code/spark080/config/config.ini")
    result = config.get(group, req_var)
    return result

def db_conn(charset=True):
    host = get_config('MYSQL', 'MYSQL_HOST')
    user = get_config('MYSQL', 'MYSQL_USER')
    password = get_config('MYSQL', 'MYSQL_PWD')
    database = get_config('MYSQL', 'MYSQL_DB')
    port = get_config('MYSQL', 'MYSQL_PORT')
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database, port=port)
    return conn

def run_tmdb_command(date_gte, movie_code, script_path):
    command = ["sh", "/home/spark/code/spark080/sh/tmdb_pyspark.sh", script_path, date_gte, movie_code]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")

start_date = '2000-01-07'
start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

# 병렬로 실행할 명령 스크립트 경로
script_paths = [
    "/home/spark/code/spark080/src/Tmdb_transform_details.py",
    "/home/spark/code/spark080/src/Tmdb_transform_credit.py",
    "/home/spark/code/spark080/src/Tmdb_transform_images.py",
    "/home/spark/code/spark080/src/Tmdb_transform_similar.py",
]

# 스레드 수
num_threads = 4

while start_datetime.year < 2001:
    query_date = start_datetime.strftime('%Y-%m-%d')
    get_movieCode_query = f"SELECT * FROM movie WHERE date_gte = '{query_date}'"
    start_datetime += timedelta(days=7)

    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute(get_movieCode_query)
    datas = cursor.fetchall()

    threads = []

    for data in datas:
        date_gte = data[2].strftime('%Y-%m-%d')
        movie_code = data[0]
        for script_path in script_paths:
            thread = threading.Thread(target=run_tmdb_command, args=(date_gte, movie_code, script_path))
            threads.append(thread)
            if len(threads) >= num_threads:
                # 지정된 스레드 수에 도달하면 스레드를 시작하고 초기화
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                threads = []

    # 남은 스레드 실행
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
