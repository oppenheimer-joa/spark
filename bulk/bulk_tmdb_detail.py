import subprocess, mysql.connector, configparser
from datetime import datetime, timedelta

def get_config(group, req_var):
	config = configparser.ConfigParser()
	config.read("/home/ubuntu/sms/test/config/config.ini")
	#config.read('/Users/jesse/Documents/sms/spark/config/config.ini')
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

start_date = '2000-01-07'
start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

while start_datetime.year < 2001:
	query_date = start_datetime.strftime('%Y-%m-%d')
	get_movieCode_query = f"SELECT * FROM movie WHERE date_gte = '{query_date}'"
	start_datetime += timedelta(days=7)
	conn = db_conn()
	cursor = conn.cursor()
	cursor.execute(get_movieCode_query)
	datas = cursor.fetchall()
	for i in range(len(datas)):
		movie_code = datas[i][0]
		date_gte = datas[i][2].strftime('%Y-%m-%d')
		#command = f"sh /home/ubuntu/sms/test/sh/tmdb_pyspark.sh /home/ubuntu/sms/test/src/Tmdb_transform_details.py {date_gte} {movie_code}"
		command = ["sh", "/home/ubuntu/sms/test/sh/tmdb_pyspark.sh", "/home/ubuntu/sms/test/src/Tmdb_transform_details.py", date_gte, movie_code]
		try:
			subprocess.run(command)
			
		except subprocess.CalledProcessError as e:
			print(f"err {command}")

