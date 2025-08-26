from pyspark.sql import SparkSession
from pyspark.sql.types import *

# change this parameter accordingly to your machine
FILE_DIR = "file:///home/p4stwi2x/Desktop/abs/taxi-data/"

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# main query

csvSchema = StructType([StructField("color", StringType(), True)])

csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", True)\
    .schema(csvSchema) \
    .csv(FILE_DIR)

wordCounts = csvDF.select("*").groupBy(csvDF.color).count()

# start query

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()