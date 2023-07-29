from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from sparksession import initilize_sparksession
# Create a SparkSession
spark = initilize_sparksession()

# Define the schema for the Parquet file
custom_schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", FloatType(), True),
    StructField("Symbol", StringType(), True),
    StructField("Security Name", StringType(), True),
    StructField("vol_moving_avg", FloatType(), True),
    StructField("adj_close_rolling_med", FloatType(), True)
])

# Path to save processed dataset
featured_stocks_path = 'dags/data/featuresAdded_stocks_etfs/'

def adding_features(file):
    name = file.stem
    df = spark.read.parquet(file)

    # Calculate volume moving average using Window function
    window_spec = Window.orderBy(F.col('Date')).rowsBetween(-29, 0)
    df = df.withColumn('vol_moving_avg', F.avg('Volume').over(window_spec))

    # Calculate adjusted close rolling median using Window function
    df = df.withColumn('adj_close_rolling_med', F.expr('percentile_approx(`Adj Close`, 0.5) OVER (ORDER BY Date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)'))

    # Drop rows with null values
    df = df.dropna(subset=features)

    # Save the DataFrame as a Parquet file
    output_file = f"{featured_stocks_path}/{name}.parquet"
    df.select(features).write.parquet(output_file, mode='overwrite', compression='snappy')

# Note: replace 'file' with the actual path to your Parquet file
# e.g., adding_features(Path('/path/to/your_parquet_file.parquet'))
