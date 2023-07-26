from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import input_file_name, lit, col, isnull
from pyspark.sql import functions as F

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