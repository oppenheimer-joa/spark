import configparser, boto3, json

def get_config(group, req_var):
    config = configparser.ConfigParser()
    # EC2 SPARK MASTER 인스턴스 경로
    #config.read('/home/ubuntu/sms/test/config/config.ini')
    config.read('/home/spark/spark_code/config/config.ini')
    #config.read('/Users/jesse/Documents/sms/spark/config/config.ini')
    result = config.get(group, req_var)
    return result

def create_s3client():
    access = get_config("AWS", "S3_ACCESS")
    secret = get_config("AWS", "S3_SECRET")

    # s3 client 생성
    s3 = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    return s3


#TMDB_peopleDetails_999606_1960-01-22.json
def make_tmdb_file_dir(category, date, code):
    if category == "detail":
        return f'TMDB/{category}/{date}/TMDB_movieDetails_{code}_{date}.json'
    elif category == "credit":
        return f'TMDB/{category}/{date}/TMDB_movieCredits_{code}_{date}.json'
    elif category == "similar":
        return f'TMDB/{category}/{date}/TMDB_movieSimilar_{code}_{date}.json'
    elif category == "image":
        return f'TMDB/{category}/{date}/TMDB_movieImages_{code}_{date}.json'
    elif category == "people":
    	return f"TMDB/{category}/{date}/TMDB_peopleDetails_{code}_{date}.json"
    else:
        return "wrong"

def get_TMDB_data(file_key):
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