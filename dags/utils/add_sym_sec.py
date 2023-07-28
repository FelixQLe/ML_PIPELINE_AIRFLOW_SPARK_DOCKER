from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
import os
from multiprocessing import cpu_count
from load_files import load_file #function load files into batches

#Create a spark Context class, with custom config to optimize the performance
conf = SparkConf()
#conf.set('spark.default.parallelism', 700)
#conf.set('spark.sql.shuffle.partitions', 700)
conf.set('spark.sql.adaptive.coalescePartitions.initialPartitionNum', 24)
conf.set('spark.sql.adaptive.coalescePartitions.parallelismFirst', 'false')
conf.set('spark.sql.files.minPartitionNum', 1)
conf.set('spark.sql.files.maxPartitionBytes', '500mb')
conf.set('spark.driver.memory', '30g')
conf.set('spark.driver.cores', 8)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.memory', '30g')
sc = SparkContext.getOrCreate(conf)

## Initialize SparkSession
spark = SparkSession.builder.master('local[*]').\
                config('spark.sql.debug.maxToStringFields', '100').\
                appName("ETFs Spark Airflow Docker").getOrCreate()

#Mapping dict
meta_symbol = spark.read.csv("dags/data/symbols_valid_meta.csv", header=True)
symbol_mapping = meta_symbol.select("Symbol", "Security Name").rdd.collectAsMap()

#stock dir
stocks_dir = "dags/data/stocks_etfs"
#processed data dir
processed_stocks_dir = "dags/data/processed_stocks_etfs"

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

def add_sym_sec_name(input_file):
    """
    Function adds Symbol and Security Name to stock file
    """
    # Read data from CSV into the DataFrame using the existing schema
    stock_df = spark.read.csv(str(input_file), header=True, schema=existing_schema)

    # Get Symbol name from input file
    symbol_name = os.path.splitext(os.path.basename(input_file))[0]

    # Adding Symbol and Security Name
    stock_df = stock_df.withColumn("Symbol", F.lit(symbol_name))
    stock_df = stock_df.withColumn("Security Name", F.lit(symbol_mapping.get(symbol_name)))

    # Save the preprocessed data to a parquet file
    output_file = os.path.join(processed_stocks_dir, f"{symbol_name}_preprocessed.parquet")
    stock_df.write.mode("overwrite").parquet(output_file)


def preprocessing_data():
    '''
    Takes batch number as input
    Map function add_sym_sec_name for every dataframe in batch number in preprocessing_list
    '''
    #list of loaded csv files will split into n_processor, for parralezation process in Airflow
    n_processor = cpu_count()
    #get batches of data
    preprocessing_list = load_file(n_processor, stocks_dir, 'csv')
    list(map(add_sym_sec_name, preprocessing_list))

