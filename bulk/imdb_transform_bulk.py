import subprocess

def run_imdb_pyspark(year:str, festa_name:str):
	spark_submit_cmd = [
				"/home/ubuntu/spark/spark-3.2.4/bin/spark-submit",
				"--master",
				"spark://master:7077",
				"/home/ubuntu/sms/test/src/Imdb_transform.py",
				year,
				festa_name
			]

			# subprocess를 사용하여 Spark-submit 명령을 실행합니다.
	try:
		subprocess.check_call(spark_submit_cmd)
		print("Spark-submit 명령이 성공적으로 실행되었습니다.")
	except subprocess.CalledProcessError as e:
		print("Spark-submit 명령 실행 중 오류가 발생했습니다.")
		print(e)

festa_list = ["academy","busan","cannes","venice"]

for festa in festa_list:
	
	if festa == "academy":
		for i in range(1961,2024):
			year = str(i)
			run_imdb_pyspark(year=year, festa_name=festa)

	elif festa == "busan":
		for i in range(1997,2023):
			year = str(i)
			run_imdb_pyspark(year=year, festa_name=festa)

	elif festa == "cannes":
		for i in range(1961,2024):
			year = str(i)
			run_imdb_pyspark(year=year, festa_name=festa)

	elif festa == "venice":
		for i in range(1961,2023):
			year = str(i)
			run_imdb_pyspark(year=year, festa_name=festa)