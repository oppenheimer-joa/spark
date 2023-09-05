import configparser, boto3

def get_config(group, req_var):
	config = configparser.ConfigParser()
	config.read('config/config.ini')
	result = config.get(group, req_var)

	return result

def create_s3client():
    
    access = get_config("AWS", "S3_ACCESS")
    secret = get_config("AWS", "S3_SECRET")

    # s3 client 생성
    s3 = boto3.client('s3', aws_access_key_id=access,
        aws_secret_access_key=secret)
        
    return s3