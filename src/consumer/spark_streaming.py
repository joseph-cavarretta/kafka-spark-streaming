from pyspark.sql import SparkSession
import consumer as consumer


if __name__ == "__main__":
    config_path = 'config.json'

    spark = SparkSession.builder \
            .appName("Spark Avro Consumer") \
            .getOrCreate()

    consumer = consumer.SparkAvroConsumer(spark, config_path)
    consumer.process_stream()
