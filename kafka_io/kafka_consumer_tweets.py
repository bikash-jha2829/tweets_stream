import re
from datetime import datetime
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from utils.utils import load_config_yaml

conf = load_config_yaml()

config = {'bootstrap.servers': conf['kafka']['servers'], 'group.id': conf['kafka']['groupid'],
          'session.timeout.ms': conf['kafka']['timeout'],
          'auto.offset.reset': conf['kafka']['offset_reset']}

schema = StructType(
    [StructField("created_at", StringType()),
     StructField("message", StringType())])


def getSparkSessionInstance(spark_context):
    # Creating the gloabal instance of SQL context only once
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SQLContext(spark_context)
    return globals()['sparkSessionSingletonInstance']


def process_data_hashtags(kafka_topic):
    try:
        # Get the Spark SQL context
        spark = SparkSession \
            .builder \
            .appName("StreamingApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    except Exception as err:
        raise err

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema)) \
        .select('created_at', 'value.id', 'value.geo', 'value.coordinates', 'value.place')
    # Changing datetime format
    date_process = udf(
        lambda x: datetime.strftime(
            datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )
    df = df.withColumn("created_at", date_process(df.created_at))
    # if cleaning is required
    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '',
                         x.lower().strip()).split(), ArrayType(StringType())
    )
    df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

    checkpoint = './twitter/events'
    query = df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint) \
        .start("s3://deltabucket/XXXX")

    query.awaitTermination()

    # testing purpose
    '''
    query = prediction \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    query.awaitTermination()
    '''


if __name__ == '__main__':
    tweet_topic = conf['kafka']['topic']
    process_data_hashtags(tweet_topic)
