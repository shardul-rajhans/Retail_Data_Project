# Importing the required libraries and setting up the environment.
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")


# Function to check is_order column
def is_order(order):
    return 1 if (order == "ORDER") else 0


# Function to check is_return column
def is_return(order):
    return 1 if (order == "RETURN") else 0


# Function to calculate total_items column
def total_items(items):
    return len(items)


# Function to calculate total_cost column
def total_cost(items, order_type):
    complete_cost = 0
    for item in items:
        complete_cost = complete_cost + item.unit_price * item.quantity if (order_type == "ORDER") else (
                complete_cost - (item.unit_price * item.quantity))
    return complete_cost


# Main Function
if __name__ == "__main__":
    # Reading the Hostname, Port, and Topic Name from Command Line Arguments.
    # spark2-submit --jar JAR_FILE_PATH HOSTNAME PORT TOPIC_NAME
    hostname = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]

    # Creating Spark Session
    spark = SparkSession \
        .builder \
        .appName("RetailDataProject") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Read Input From Kafka
    orderRaw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", hostname + ":" + port) \
        .option("subscribe", topic) \
        .load() \
        .selectExpr("CAST(value AS STRING) as data")

    # Creating the Schema for the JSON.
    jsonSchema = StructType() \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("quantity", IntegerType())]))
             ) \
        .add("type", StringType()) \
        .add("country", StringType()) \
        .add("invoice_no", LongType()) \
        .add("timestamp", TimestampType())

    # Creating the dataframe from input after applying the schema and selecting the required columns.
    orderStream = orderRaw.select(from_json(col("data"), jsonSchema).alias("order")).select("order.*")

    # Converting Functions to UDFs
    isOrderUDF = udf(is_order, IntegerType())
    isReturnUDF = udf(is_return, IntegerType())
    totalItemsUDF = udf(total_items, IntegerType())
    totalCostUDF = udf(total_cost, DoubleType())

    # Adding new Attributes calculated using UDFs.
    orderStream = orderStream.withColumn("is_order", isOrderUDF(orderStream.type)) \
        .withColumn("is_return", isReturnUDF(orderStream.type)) \
        .withColumn("total_items", totalItemsUDF(orderStream.items)) \
        .withColumn("total_cost", totalCostUDF(orderStream.items, orderStream.type))

    # Calculating Time Based KPIs
    timeBasedKPIs = orderStream.withWatermark("timestamp", "1 minute") \
        .groupby(window("timestamp", "1 minute")) \
        .agg(count("invoice_no").alias("OPM"),
             sum("total_cost").alias("total_sale_volume"),
             sum("is_return").alias("returns"),
             sum("is_order").alias("orders")) \
        .withColumn("rate_of_return", col("returns") / (col("returns") + col("orders"))) \
        .withColumn("average_transaction_size", col("total_sale_volume") / (col("returns") + col("orders")))

    # Calculating Time and Country Based KPIs
    timeAndCountryBasedKPIs = orderStream.withWatermark("timestamp", "1 minute") \
        .groupby(window("timestamp", "1 minute"), "country") \
        .agg(count("invoice_no").alias("OPM"),
             sum("total_cost").alias("total_sale_volume"),
             sum("is_return").alias("returns"),
             sum("is_order").alias("orders")) \
        .withColumn("rate_of_return", col("returns") / (col("returns") + col("orders")))

    # Extracting required Columns for Console Output.
    orderStream = orderStream.selectExpr("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order",
                                         "is_return")
    # Writing the orderOutputStream to Console.
    orderOutputStream = orderStream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

    # Extracting required Columns for timeBasedKPIs.
    timeBasedKPIs = timeBasedKPIs.selectExpr("window", "OPM", "total_sale_volume", "average_transaction_size",
                                             "rate_of_return")
    # Writing the timeBasedKPIs as json files.
    timeBasedKPIsOutput = timeBasedKPIs \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/tmp/TimeBasedKPI") \
        .option("checkpointLocation", "/tmp/CPLTimeBased") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

    # Extracting required Column for timeAndCountryBasedKPIs
    timeAndCountryBasedKPIs = timeAndCountryBasedKPIs.selectExpr("window", "country", "OPM", "total_sale_volume",
                                                                 "rate_of_return")

    # Writing the Time and Country Based KPIs as json files.
    timeAndCountryBasedKPIsOutput = timeAndCountryBasedKPIs \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/tmp/TimeAndCountryBasedKPI") \
        .option("checkpointLocation", "/tmp/CPLTimeAndCountryBased") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

    # Waiting infinitely for the interrupt
    orderOutputStream.awaitTermination()
    timeBasedKPIsOutput.awaitTermination()
    timeAndCountryBasedKPIsOutput.awaitTermination()

