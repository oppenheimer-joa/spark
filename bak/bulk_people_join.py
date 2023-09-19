import subprocess, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def run_tmdb_people_command(date_gte):
    command = ["sh", "/home/spark/spark_code/sh/tmdb_people_join_pyspark.sh", date_gte]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")

#start_date = '1961-01-20'
#start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

start_year='1960'
start_year_dt=datetime.strptime(start_year,'%Y')

while start_year_dt.year < 2023:
    date_gte=start_datetime.strftime('%Y')
    run_tmdb_people_command(date_gte)
    print(date_gte,'완료>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    start_datetime += relativedelta(years==1)