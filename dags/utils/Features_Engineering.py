#####
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import functions as F
from sparksession import initilize_sparksession
from load_files import load_file
from multiprocessing import cpu_count

# Create a SparkSession
spark = initilize_sparksession()

#processed data dir
processed_stocks_dir = "dags/data/processed_stocks_etfs"
#featured data dir
featured_stocks_dir = 'dags/data/featuresAdded_stocks_etfs/'

#list of loaded csv files will split into n_processor batches, for parralezation data processing in Airflow
n_processor = cpu_count()
#get batches of data
preprocessing_list = load_file(n_processor, processed_stocks_dir, 'parquet')

def adding_features(input_file, spark):
    """
    Function take input 
    spark :: sparksession and 
    input_file :: add two features, volume moving average and median to the input file
    """
    name = input_file.stem
    processed_stock = spark.read.parquet(str(input_file))

    # Calculate volume moving average using Window function for the last 30 days, including the current row
    w_date = Window.partitionBy(F.lit(0)).orderBy(F.col('Date')).rowsBetween(-29, 0)
    processed_stock = processed_stock.withColumn('vol_moving_avg', F.mean('Volume').over(w_date))
    processed_stock = processed_stock.withColumn('vol_moving_avg', F.round('vol_moving_avg', 0))
    # Calculate the rolling median for the 'Volume' column over the last 30 days
    processed_stock = processed_stock.withColumn('adj_close_rolling_med', F.expr('percentile(Volume, 0.5)').over(w_date))
    processed_stock = processed_stock.withColumn('adj_close_rolling_med', F.round('adj_close_rolling_med', 0))
    #drop the first 30 days
    featured_stock = processed_stock.withColumn("counter", F.monotonically_increasing_id())
    w_counter = Window.partitionBy(F.lit(0)).orderBy("counter")
    featured_stock = featured_stock.withColumn("index", F.row_number().over(w_counter))
    featured_stock = featured_stock.filter(F.col("index") >= 30)
    featured_stock = featured_stock.drop("counter", "index")

    # Save the DataFrame as a Parquet file
    output_file = f"{featured_stocks_dir}/{name}_featured.parquet"
    featured_stock.write.mode("overwrite").option("compression", "snappy").parquet(output_file)

def featuring_data(batch_number:int, sparksession):
    '''
    Takes batch number as input
    Map function add_sym_sec_name for every dataframe in batch number in preprocessing_list
    '''
    list_spark = [sparksession]*len(preprocessing_list[batch_number]) #list of initializing SparkSesssion for mapping working
    list(map(adding_features, preprocessing_list[batch_number], list_spark))


for i in range(8):
    featuring_data(i, spark)