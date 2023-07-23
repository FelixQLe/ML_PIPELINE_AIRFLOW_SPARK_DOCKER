import pandas as pd
from utils.save_parquet import save_parquet

#read metal symbol files
metal_symbol = pd.read_csv('dags/data/symbols_valid_meta.csv')
metal_symbol = metal_symbol[['Symbol', 'Security Name']]
metal_symbol['Symbol'] = metal_symbol['Symbol'].str.replace('$', '-',regex=False)
metal_symbol['Symbol'] = metal_symbol['Symbol'].str.replace('.V', '',regex=False)
#creat mapping dictionary
symbol_mapping = metal_symbol.set_index('Symbol').to_dict()['Security Name']
#retain features columns
features = ['Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
#path to save processed dataset
path = 'dags/data/processed_stocks_etfs/'

def add_name(file):
    name = file.stem
    df = pd.read_csv(file)
    df['Symbol'] = name
    df['Security Name'] = df['Symbol'].map(symbol_mapping)
    #df.name = name
    #return df
    save_parquet(df[features], name, path)


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType

# Step 1: Create a SparkSession
spark = SparkSession.builder.appName("ETFs_Stocks_Data").getOrCreate()

# Step 2: Define the schema
schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Security Name", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", IntegerType(), True)
])

# Step 3: Create an empty DataFrame with the defined schema
etf_stocks_df = spark.createDataFrame([], schema)

# Step 4: Read data from CSV into the DataFrame
file_path = "path/to/etf_stock_data.csv"  # Update this with the actual path of your CSV file
etf_stocks_df = spark.read.csv(file_path, header=True, schema=schema)

# Now, the 'etf_stocks_df' DataFrame contains your data with the specified columns and data types.
# You can perform various operations on this DataFrame using Spark's APIs.
