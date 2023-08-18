from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pandas import pd

spark = SparkSession.builder.appName("DataExtraction").getOrCreate() 

#Define Schema for the data
existing_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", FloatType(), True),
    StructField("Symbol", FloatType(), True),
    StructField("Security Name", FloatType(), True)
])

#stock = pd.read_csv('./dags/data/A.csv')
stock_df = spark.read.csv("./dags/data/A.csv", header=True, schema=existing_schema)

stock_df = stock_df.withColumn("Symbol", F.lit("A"))

 # Save the preprocessed data to a parquet file
stock_df.write.mode("overwrite").option("compression", "snappy").parquet("./dags/data/A_preprocessed.parquet")
