from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


#Create a spark Context class, with custom config to optimize the performance
conf = SparkConf()
#conf.set('spark.default.parallelism', 700)
#conf.set('spark.sql.shuffle.partitions', 700)
conf.set('spark.sql.adaptive.coalescePartitions.initialPartitionNum', 24)
conf.set('spark.sql.adaptive.coalescePartitions.parallelismFirst', 'false')
conf.set('spark.sql.adaptive.enabled', 'false')
conf.set('spark.sql.files.minPartitionNum', 1)
conf.set('spark.sql.files.maxPartitionBytes', '500mb')
conf.set('spark.driver.memory', '30g')
conf.set('spark.driver.cores', 8)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.memory', '30g')
sc = SparkContext.getOrCreate(conf)

def initilize_sparksession():

    ## Initialize SparkSession
    return SparkSession.builder.master('local[*]').\
                    appName("ETFs Spark Airflow Docker").getOrCreate()
    