from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").\
    config("spark.hadoop.fs.s3a.access.key", "AKIA54TZQARETLAG2EWQ") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .getOrCreate()

s3_path = "s3a://sms-basket/TMDB/credit/1960-01-01/TMDB_movieCredits_1000336_1960-01-01.json"

json_df = spark.read.json(s3_path)

json_df.show()
spark.stop()