import configparser, boto3
from xml_to_dict import XMLtoDict
import json
import io

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

def get_raw_data(date):
    year = date.split('-')[0]
    print(date,year)
    s3 = create_s3client()
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket='sms-basket',Prefix=f'kopis/{year}')

    file_list = []

    for page in page_iterator : 
        file_list += [obj['Key'] for obj in page['Contents'] if obj['Key'].find(date)>-1]
    
    xml_file_list=[]

    for file in file_list:
        obj=s3.get_object(Bucket='sms-basket', Key = file)

        result=io.BytesIO(obj['Body'].read())
        wrapper = io.TextIOWrapper(result, encoding='utf-8')
        text_xml = wrapper.read()

        parsing_info=XMLtoDict().parse(text_xml)['dbs']['db']

        parsing_json=json.dumps(parsing_info, ensure_ascii=False, indent=2, separators=(',', ': '))

        xml_file_list.append(parsing_json)
        # xml_file_list.append(str(parsing_info))

    return xml_file_list
