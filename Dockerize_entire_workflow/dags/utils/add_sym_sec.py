from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import input_file_name, lit, col, isnull
from pyspark.sql import functions as F
import os

#Create a spark Context class, with custom config
conf = SparkConf()
conf.set('spark.default.parallelism', 700)
conf.set('spark.sql.shuffle.partitions', 700)
conf.set('spark.driver.memory', '30g')
conf.set('spark.driver.cores', 8)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.memory', '30g')
sc = SparkContext.getOrCreate(conf)

## Initialize SparkSession
spark = SparkSession.builder.master('local[*]').\
                config('spark.sql.debug.maxToStringFields', '100').\
                appName("ETFs Spark Airflow Docker").getOrCreate()


def preprocessing_data(spark, input_file, output_dir, symbol_mapping):
    #Define Schema for the data
    existing_schema = StructType([
        StructField("Date", StringType(), False),
        StructField("Open", FloatType(), False),
        StructField("High", FloatType(), False),
        StructField("Low", FloatType(), False),
        StructField("Close", FloatType(), False),
        StructField("Adj Close", FloatType(), False),
        StructField("Volume", FloatType(), False),
        StructField("Symbol", FloatType(), False),
        StructField("Security Name", FloatType(), False)

    ])
    # Read data from CSV into the DataFrame using the existing schema
    stock_df = spark.read.csv(input_file, header=True, schema=existing_schema)

    # Get Symbol name from input file
    symbol_name = os.path.splitext(os.path.basename(input_file))[0]

    # Adding Symbol and Security Name
    stock_df = stock_df.withColumn("Symbol", F.lit(symbol_name))
    stock_df = stock_df.withColumn("Security Name", F.lit(symbol_mapping.get(symbol_name)))

    # Save the preprocessed data to a parquet file
    output_file = os.path.join(output_dir, f"{symbol_name}_preprocessed.parquet")
    stock_df.write.parquet(output_file, header=True, mode="over")


if __name__ == "__main__":


    input_dir = "../data/stocks_etfs/A.csv"