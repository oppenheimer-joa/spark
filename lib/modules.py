import configparser, mysql.connector, boto3,json

def get_config(group, req_var):
	config = configparser.ConfigParser()
	config.read('/Users/woorek/Documents/sms/spark/config/config.ini')
	result = config.get(group, req_var)

	return result


def db_conn(charset=True):

	host = get_config('MYSQL', 'MYSQL_HOST')
	user = get_config('MYSQL', 'MYSQL_USER')
	password = get_config('MYSQL', 'MYSQL_PWD')
	database = get_config('MYSQL', 'MYSQL_DB')
	port = get_config('MYSQL', 'MYSQL_PORT')

	if charset:
		conn = mysql.connector.connect(host=host,
									user=user,
									password=password,
									database=database,
									port=port)

	else :
		conn = mysql.connector.connect(host=host,
									user=user,
									password=password,
									database=database,
									port=port,
									charset='utf8mb4')

	return conn

def create_s3client():
	access = get_config("AWS", "S3_ACCESS")
	secret = get_config("AWS", "S3_SECRET")

	# s3 client 생성
	s3 = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)

	return s3


def make_imdb_file_dir(festa_name, year):
	if festa_name == "academy":
		return f'IMDb/imdb_{festa_name}_{year}.json'
	elif festa_name == "busan":
		return f'IMDb/imdb_{festa_name}_{year}.json'
	elif festa_name == "cannes":
		return f'IMDb/imdb_{festa_name}_{year}.json'
	elif festa_name == "venice":
		return f'IMDb/imdb_{festa_name}_{year}.json'
	else:
		return "wrong"

def get_s3_data(file_key):
	if file_key == "wrong":
		return "wrong_category"
	else:
		s3 = create_s3client()

	try:
		obj = s3.get_object(Bucket='sms-basket', Key=file_key)
		raw_data = obj['Body'].read()
		json_data = json.loads(raw_data.decode('utf-8'))
		return json.dumps(json_data)
	except Exception as e:
		print(str(e))