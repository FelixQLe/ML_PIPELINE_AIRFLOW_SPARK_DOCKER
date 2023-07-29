from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
import os
from multiprocessing import cpu_count
from load_files import load_file #function load files into batches
#from SparkSession import initilize_sparksession

#stock dir
stocks_dir = "dags/data/stocks_etfs"
#processed data dir
processed_stocks_dir = "dags/data/processed_stocks_etfs"

#list of loaded csv files will split into n_processor batches, for parralezation data processing in Airflow
n_processor = cpu_count()
#get batches of data
preprocessing_list = load_file(n_processor, stocks_dir, 'csv')

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
#spark = initilize_sparksession()
def add_sym_sec_name(input_file, spark):
    """
    Function adds Symbol and Security Name to stock file
    """
    #Mapping dict
    meta_symbol = spark.read.csv("dags/data/symbols_valid_meta.csv", header=True)
    symbol_mapping = meta_symbol.select("Symbol", "Security Name").rdd.collectAsMap()

    # Read data from CSV into the DataFrame using the existing schema
    stock_df = spark.read.csv(str(input_file), header=True, schema=existing_schema)

    # Get Symbol name from input file
    symbol_name = os.path.splitext(os.path.basename(input_file))[0]

    # Adding Symbol and Security Name
    stock_df = stock_df.withColumn("Symbol", F.lit(symbol_name))
    stock_df = stock_df.withColumn("Security Name", F.lit(symbol_mapping.get(symbol_name)))

    # Save the preprocessed data to a parquet file
    output_file = os.path.join(processed_stocks_dir, f"{symbol_name}_preprocessed.parquet")
    stock_df.write.mode("overwrite").option("compression", "snappy").parquet(output_file)

def duplicate_n_times(input_list, n):
    # Using list comprehension to duplicate each item 'n' times
    duplicated_list = [item for item in input_list for _ in range(n)]
    return duplicated_list


def preprocessing_data(batch_number:int, sparksession):
    '''
    Takes batch number as input
    Map function add_sym_sec_name for every dataframe in batch number in preprocessing_list
    '''
    list_spark = duplicate_n_times([sparksession], len(preprocessing_list[batch_number])) #list of initializing SparkSesssion for mapping
    list(map(add_sym_sec_name, preprocessing_list[batch_number], list_spark))


#preprocessing_data(0, spark)