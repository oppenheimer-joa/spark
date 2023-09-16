import json, sys
sys.path.append('/home/spark/spark_code')
from lib.modules import *
from pyspark.sql import SparkSession

access = get_config('AWS', 'S3_ACCESS')
secret = get_config('AWS', 'S3_SECRET')

# Spark session 초기화
spark = SparkSession.builder \
    .appName("BoxOfficeJsonToParquet") \
    .config("spark.hadoop.fs.s3a.access.key", access) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

#date_foramt : yyyy-mm-dd
date = sys.argv[1]

# 박스오피스 wholeText로 다 가져오고, 거기서 앞에 날짜를 기반으로 해당 json만 읽어오기
s3_path = spark.sparkContext.wholeTextFiles(f"s3a://sms-basket/kobis/{date.split('-')[0]}")
filtered_files = s3_path.filter(lambda filter_data: date.replace('-', '') in filter_data[0])

# 읽은 json 파일 dataframe으로 전부다 합치기 지역코드 추가해야함

def transform_boxOffice_data(file_name, json_data):
    try:
        data = json.loads(json_data)
        result = []
        daily_boxOffice = data.get("boxOfficeResult", {}).get("dailyBoxOfficeList", [])
        for i in range(len(daily_boxOffice)):
            location_code = file_name.split('/')[5].split('_')[1]
            rank = daily_boxOffice[i]["rank"]
            movie_name = daily_boxOffice[i]["movieNm"]
            movie_open = daily_boxOffice[i]["openDt"]
            sales_amount = daily_boxOffice[i]["salesAmt"]
            sales_share = daily_boxOffice[i]["salesShare"]
            sales_inten = daily_boxOffice[i]["salesInten"]
            sales_change = daily_boxOffice[i]["salesChange"]
            sales_acc = daily_boxOffice[i]["salesAcc"]
            audi_cnt = daily_boxOffice[i]["audiCnt"]
            audi_inten = daily_boxOffice[i]["audiInten"]
            audi_change = daily_boxOffice[i]["audiChange"]
            audi_acc = daily_boxOffice[i]["audiAcc"]
            scrn_cnt = daily_boxOffice[i]["scrnCnt"]
            show_cnt = daily_boxOffice[i]["showCnt"]
            result.append((location_code, date, rank, movie_name, movie_open, sales_amount, sales_share, sales_inten, sales_change,\
                sales_acc, audi_cnt, audi_inten, audi_change, audi_acc, scrn_cnt, show_cnt))
        return result
    except json.JSONDecodeError as e:
        return (f"json decode err : {e}")


# 만들어진 df를 temp에 떨어뜨려야함

#tmp = filtered_files.values().map(transform_boxOffice_data)
box_office_rdd = filtered_files.flatMap(lambda datas : transform_boxOffice_data(datas[0], datas[1]))

result_df = spark.createDataFrame(box_office_rdd, ["loc_code","date", "rank", "movie_nm", "movie_open",\
    "sales_amount", "sales_share", "sales_inten", "sales_change", "sales_acc", \
    "audi_cnt", "audi_inten", "audi_change", "audi_acc", "scrn_cnt", "show_cnt"])

s3_path = f's3a://sms-warehouse/temp/kobis'
filename = f'boxOffice_{date}'
result_df.write.mode("overwrite").parquet(f'{s3_path}/{filename}')
