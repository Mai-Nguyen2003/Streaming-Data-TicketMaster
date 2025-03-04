from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

from jobs.configuration import configuration
def main():
    spark = SparkSession.builder.appName('EventStreaming')\
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5," 
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .getOrCreate()

    #Adjust the log level to minimize the console output
    spark.sparkContext.setLogLevel("WARN")

    # Define schemas

    event_schema = StructType([
        StructField("id",StringType(),nullable=True),
        StructField("event_name",StringType(),nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("locale", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True)])

    sales_schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("Start_SalesDate", DateType(), nullable=True),
        StructField("End_SalesDate", DateType(), nullable=True),
        StructField("minPrice", DoubleType()),
        StructField("maxPrice", DoubleType())])

    classification_schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("segment", StringType(), nullable=True),
        StructField("genre", StringType(), nullable=True),
        StructField("subGenre", StringType(), nullable=True)])

    venue_schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("place", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("postalCode", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True)])

    attraction_schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("attraction", StringType(), nullable=True),
        StructField("attraction_segment", StringType(), nullable=True),
        StructField("attraction_genre", StringType(), nullable=True),
        StructField("attraction_subGenre", StringType(), nullable=True)])

    def read_kafka_topic(topic,schema):
        return (spark.readStream
                .format("kafka")
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe',topic)
                .option('startingOffsets', 'latest')
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'),schema).alias('data'))
                .select('data.*'))

    def streamWriter(input: DataFrame, checkpoint_folder, output):
        return (input.writeStream
                .outputMode('append')
                .format('parquet')
                .option('path', output)
                .option('checkpointLocation', checkpoint_folder)
                .start())


    event_df = read_kafka_topic('event_data', event_schema).alias('event')
    sales_df = read_kafka_topic('sales_data', sales_schema).alias('sales')
    classification_df = read_kafka_topic('classification_data', classification_schema).alias('classification')
    venue_df = read_kafka_topic('venue_data', venue_schema).alias('venue')
    attraction_df = read_kafka_topic('attraction_data', attraction_schema).alias('attraction')


    query1 = streamWriter(event_df,"s3a://spark-ticketmaster/checkpoints/event_data","s3a://spark-ticketmaster/data/event_data")
    query2 = streamWriter(sales_df,"s3a://spark-ticketmaster/checkpoints/sales_data","s3a://spark-ticketmaster/data/sales_data")
    query3 = streamWriter(classification_df,"s3a://spark-ticketmaster/checkpoints/classification_data","s3a://spark-ticketmaster/data/classification_data")
    query4= streamWriter(venue_df,"s3a://spark-ticketmaster/checkpoints/venue_data","s3a://spark-ticketmaster/data/venue_data")
    query5 = streamWriter(attraction_df,"s3a://spark-ticketmaster/checkpoints/attraction_data","s3a://spark-ticketmaster/data/attraction_data")

    query5.awaitTermination()

if __name__ == '__main__':
    main()
