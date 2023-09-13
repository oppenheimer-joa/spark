import configparser, boto3, json

def get_config(group, req_var):
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    result = config.get(group, req_var)
    return result

def create_s3client():
    access = get_config("AWS", "S3_ACCESS")
    secret = get_config("AWS", "S3_SECRET")

    # s3 client 생성
    s3 = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    return s3

def get_spotify_data(year, movie_id):
    s3 = create_s3client()

    file = f'spotify/{year}/spotify_{movie_id}_{year}.json'
    try:
        obj = s3.get_object(Bucket='sms-basket', Key=file)
        raw_data = obj['Body'].read()
        json_data = json.loads(raw_data.decode('utf-8'))
        return movie_id, json.dumps(json_data)
    except Exception as e:
        print(file, "<<<<< Not found >>>>>")
        print(str(e))


