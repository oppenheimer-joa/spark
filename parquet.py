import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

def read_parquet_from_s3(bucket_name, folder_prefix):
    from configparser import ConfigParser

    parser = ConfigParser()
    parser.read("config/config.ini")
    access = parser.get("AWS", "S3_ACCESS")
    secret = parser.get("AWS", "S3_SECRET")

    s3 = boto3.client('s3', aws_access_key_id=access,
    aws_secret_access_key=secret)
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    

    combined_df = pd.DataFrame()
    for obj in objects.get('Contents'):
        file_path = 's3://{}/{}'.format(bucket_name, obj.get('Key'))

        if file_path.find('parquet') == -1:
            continue
        
        s3_object = s3.get_object(Bucket=bucket_name, Key=obj.get('Key'))
        parquet_stream = s3_object['Body'].read()
        
        table = pq.read_table(BytesIO(parquet_stream))
        df = table.to_pandas()
        
        combined_df = combined_df.append(df, ignore_index=True)
    
    return combined_df


if __name__ == "__main__":
    bucket_name = 'sms-warehouse'
    folder_prefix = 'kobis/'
    demo_df = read_parquet_from_s3(bucket_name, folder_prefix)
    print(demo_df)
    for col in demo_df.columns:
        print(col, end= ' : ')
        print(demo_df.loc[0,col])
