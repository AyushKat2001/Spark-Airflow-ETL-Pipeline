from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ViewFile").master("local[*]").getOrCreate()

df = spark.read.parquet("/mnt/c/Users/ayush/python_data_engineering/Advanced_ETL/ETL_Pipeline/processed/")
df.show()
df.printSchema()
spark.stop()