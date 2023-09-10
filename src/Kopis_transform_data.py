import json, sys
sys.path.append('/home/ubuntu/sms/test')
from lib.modules import *
from pyspark.sql import SparkSession
from xml_to_dict import XMLtoDict

# Spark Session Build

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("KopisXmlToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

print("spark session built successfully")

def get_raw_data(date):
    year = date.split('-')[0]

    # date를 포함하는 파일 검색 → (파일 경로, 파일 내용) 튜플 형태로 저장한 RDD 반환
    file_list = spark.sparkContext.wholeTextFiles(f"s3a://sms-basket/kopis/{year}/*{date}*.xml")

    parsing_list = []

    # xml -> dictionary -> JSON 형식의 문자열 변환
    for row in file_list.collect(): # collect() : 튜플들이 로컬 컴퓨터의 메모리로 수집
        xml_string = row[1] # 파일 내용 가져오기
        parsing_info=XMLtoDict().parse(xml_string)['dbs']['db']
        parsing_json=json.dumps(parsing_info, ensure_ascii=False, indent=2, separators=(',', ': '))
        parsing_list.append(parsing_json)

    return parsing_list

# 데이터 전처리
def transform_json(json_str):
    data = json.loads(json_str)

    try :
        tksite_temp = data.get('tksites').get('tksite')

        if type(tksite_temp) == type([]):
            tksite_pro=[{tksite['#text']:tksite['@href']} for tksite in tksite_temp]
        else:
            tksite_pro=[{tksite_temp['#text']:tksite_temp['@href']}]

    except:
        tksite_pro=[]

    try:
        styurl = list(data.get('styurls').values())

        if type(styurl[0]) == type([]) :
            styurl=sum(styurl,[])

    except :
        styurl=[]

    data['styurls'] = styurl
    data['tksites'] = str(tksite_pro)

    return json.dumps(data)

# spark job
def spark_job_kopis(date):

    file_list = get_raw_data(date)

    # 데이터를 RDD로 변환
    raw_rdd = spark.sparkContext.parallelize(file_list)
    transformed_rdd = raw_rdd.map(transform_json)

    # 변환된 JSON 데이터 출력
    json_df = spark.read.json(transformed_rdd)
    json_df.show()

    # 데이터 프레임을 Parquet 파일로 저장
    output_path = f'sms-warehouse/kopis/KOPIS_{date}'
    json_df.write.parquet(f"s3a://{output_path}")

# Execute
date = sys.argv[1]
spark_job_kopis(date)
spark.stop()