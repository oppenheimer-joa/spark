import configparser, boto3, json

def get_config(group, req_var):
    config = configparser.ConfigParser()
    # EC2 SPARK MASTER 인스턴스 경로
    config.read('/home/ubuntu/sms/test/config/config.ini')
    #config.read('config/config.ini')
    result = config.get(group, req_var)
    return result

def create_s3client():
    access = get_config("AWS", "S3_ACCESS")
    secret = get_config("AWS", "S3_SECRET")

    # s3 client 생성
    s3 = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    return s3

def make_tmdb_file_dir(category, date, movie_code):
    if category == "detail":
        return f'TMDB/{category}/{date}/TMDB_movieDetails_{movie_code}_{date}.json'
    elif category == "credit":
        return f'TMDB/{category}/{date}/TMDB_movieCredits_{movie_code}_{date}.json'
    elif category == "similar":
        return f'TMDB/{category}/{date}/TMDB_movieSimilar_{movie_code}_{date}.json'
    elif category == "image":
        return f'TMDB/{category}/{date}/TMDB_movieImages_{movie_code}_{date}.json'
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

def transform_imdb(json_str):
    json_obj = json.loads(json_str)
    awards_list = json_obj["nomineesWidgetModel"]["eventEditionSummary"]["awards"]

    final_data = []

    for i in awards_list:
        award_Name = i["awardName"]
        for cate in i["categories"]:
            for nomi in cate["nominations"]:
                if nomi["isWinner"] is True :
                    award_Category = nomi["categoryName"]

                    if len(nomi["primaryNominees"]) != 0 :
                        award_Winner = nomi["primaryNominees"][0]["name"]
                        award_Image = nomi["primaryNominees"][0]["imageUrl"]
                    else:
                        pass

                    period_tuple = (award_Name, award_Category, award_Winner, award_Image)
                    final_data.append(period_tuple)
                else :
                    pass

    return final_data
