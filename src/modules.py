import configparser, boto3, json

def get_config(group, req_var):
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    #config.read('config/config.ini')
    result = config.get(group, req_var)
    return result

def create_s3client():
    access = get_config("AWS", "S3_ACCESS")
    secret = get_config("AWS", "S3_SECRET")

    # s3 client 생성
    s3 = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    return s3

def make_tmdb_file_dir(category, year, movie_code):
    if category == "detail":
        return f'TMDB/{category}/{year}/TMDB_movieDetails_{movie_code}_{year}.json'
    elif category == "credit":
        return f'TMDB/{category}/{year}/TMDB_movieCredits_{movie_code}_{year}.json'
    elif category == "similar":
        return f'TMDB/{category}/{year}/TMDB_movieSimilar_{movie_code}_{year}.json'
    elif category == "image":
        return f'TMDB/{category}/{year}/TMDB_movieImages_{movie_code}_{year}.json'
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