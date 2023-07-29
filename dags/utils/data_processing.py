from pyspark.sql.types import StructType, StructField, StringType, FloatType
#from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from multiprocessing import cpu_count
from load_files import load_file #function load files into batches
#from sparksession import initilize_sparksession

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
#spark = initilize_sparksession()
def add_sym_sec_name(input_file, spark):
    """
    Function adds Symbol and Security Name to stock file
    """
    #Mapping dictionary
    meta_symbol = spark.read.csv("dags/data/symbols_valid_meta.csv", header=True)
    symbol_mapping = meta_symbol.select("Symbol", "Security Name").rdd.collectAsMap()

    # Read data from CSV into the DataFrame using the existing schema
    stock_df = spark.read.csv(str(input_file), header=True, schema=existing_schema)

    # Get Symbol name from input file
    symbol_name = input_file.stem

    # Adding Symbol and Security Name
    stock_df = stock_df.withColumn("Symbol", F.lit(symbol_name))
    stock_df = stock_df.withColumn("Security Name", F.lit(symbol_mapping.get(symbol_name)))

    # Save the preprocessed data to a parquet file
    stock_df.write.mode("overwrite").option("compression", "snappy").parquet(processed_stocks_dir+"/"+symbol_name+"_preprocessed.parquet")

def duplicate_n_times(input_list, n):
    # Using list comprehension to duplicate each item 'n' times
    duplicated_list = [item for item in input_list for _ in range(n)]
    return duplicated_list


def preprocessing_data(batch_number:int, sparksession):
    '''
    Takes batch number as input
    Map function add_sym_sec_name for every dataframe in batch number in preprocessing_list
    '''
    list_spark = duplicate_n_times([sparksession], len(preprocessing_list[batch_number])) #list of initializing SparkSesssion for mapping working
    list(map(add_sym_sec_name, preprocessing_list[batch_number], list_spark))

#for i in range(8):
#    preprocessing_data(i, spark)