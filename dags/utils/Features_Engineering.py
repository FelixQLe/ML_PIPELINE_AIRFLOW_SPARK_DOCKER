#####
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import functions as F
from sparksession import initilize_sparksession

# Create a SparkSession
spark = initilize_sparksession()

# Path to save processed dataset
featured_stocks_path = 'dags/data/featuresAdded_stocks_etfs/'

def adding_features(input_file, spark):
    
    name = input_file.stem
    processed_stock = spark.read.parquet(input_file)

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
    output_file = f"{featured_stocks_path}/{name}.parquet"
    df.select(features).write.parquet(output_file, mode='overwrite', compression='snappy')

# Note: replace 'file' with the actual path to your Parquet file
# e.g., adding_features(Path('/path/to/your_parquet_file.parquet'))
