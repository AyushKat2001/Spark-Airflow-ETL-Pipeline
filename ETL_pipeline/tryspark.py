from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, when, current_date
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import logging
import sys
from pathlib import Path

schema = StructType(
    [
        StructField("name",StringType(),True),
        StructField("age", StringType(),True),
        StructField("salary",StringType(),True),
        StructField("active",StringType(),True)
    ]
)
LOG_DIR = Path(__file__).parent/"logs"
LOG_DIR.mkdir(parents=True,exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR/"pipelinedemo1.log",encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ],
)

def main(input_p,output_p):
    spark = SparkSession.builder.appName("MYappETL").master("local[*]").getOrCreate()
    df = spark.read.schema(schema).csv(f"{input_p}/*.csv", header=True)

    if df.rdd.isEmpty():
        raise ValueError("No csv files can be found")
    
    df = df.withColumn("age",when(col("age").isin("","NaN","nan"),None).otherwise(col("age")))
    df = df.withColumn("age",col("age").cast(IntegerType()))
    df = df.withColumn("salary",col("salary").cast("double"))
    df = df.withColumn("active",when(col("active").isin("True","true","1"),1)
                       .when(col("active").isin("False","false","0"),0))
    df = df.withColumn("load_date", current_date()) # makes a new column load_date
    
    df_valid = (
        df
        .filter((col("name").isNotNull())& (col("name")!=""))
        .filter((col("salary").isNull()) | (col("salary") >= 0))
        .filter((col("age").isNull()) | (col("age")>= 18))
    )
    df.persist() #cache the data to be reused for execution
    df_valid.persist()
    total = df.count()
    if total == 0:
        raise ValueError("Input contains headers only")
    valid = df_valid.count()
    invalid = total - valid
    threshold = invalid / total
    logging.info(f"Total of {total} rows will be validated")
    logging.info(f"{valid} rows is valid")
    if threshold > 0.25:
        raise ValueError(f"Drop ratio: {threshold:.2%} >> Dropping the file")
    df_valid.write.partitionBy("load_date").mode("overwrite").parquet(output_p) #make folders by load_date
    df_invalid = df.filter(
        (col("name").isNull()) |
        (col("name") == "") |
        ((col("salary").isNotNull()) & (col("salary") < 0)) |
        ((col("age").isNotNull()) & (col("age") < 18))
    )
    df_invalid.write.mode("overwrite").parquet(f"{output_p}/bad_records")
    
    df.unpersist() #Free's the memory 
    df_valid.unpersist()
    spark.stop()

if __name__ == "__main__":
   import sys
   input_p = sys.argv[1]
   output_p = sys.argv[2]
   main(input_p,output_p)