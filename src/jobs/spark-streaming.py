import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from time import sleep
import os
import sys 

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the parent directory
parent_dir = os.path.dirname(current_dir)

# Construct the path to the config directory
config_dir = os.path.join(parent_dir, 'config')

# Add the config directory to the system path
sys.path.append(config_dir)

from config import config

def start_streaming(spark):
    topic = "yelp-reviews"
    while True:
        try:
            # read the data from the socket
            stream_df = (spark.readStream.format("socket")
                        .option("host", "0.0.0.0")
                        .option("port", 9999).load()) # end bracket is because we are writing in multiple lines

            # Define the schema for the data    
            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            # StructType is a collection of StructField.
            # Since the socket stream is a continuous stream of data, we need to parse the data into a DataFrame.
            # currently stream_df is a DataFrame with a single column named value. we need to parse the data into a DataFrame with the specified schema.
            stream_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
            # The from_json() function is used to parse a JSON string and convert it into a DataFrame with the specified schema.
            # The select() function is used to select the columns from the DataFrame.

            # Part1 writing to terminal for test purpose later we will see how to write to Kafka
            # query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start() # This is just writing to console.
            # console is the sink where the data will be written to.
            # In this case, the data will be written to the console.
            # query.awaitTermination() # this is to keep the stream running
            # awaitTermination() is a method that waits for the termination of the query.
            # This method will block until the query is stopped either by an exception or by invoking query.stop().


            # Part2 writing to Kafka
            # write the data to the Kafka topic
            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            query = (kafka_df.writeStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                   .option("kafka.security.protocol", config['kafka']['security.protocol'])
                   .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanism'])
                   .option('kafka.sasl.jaas.config',
                           'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                           'password="{password}";'.format(
                               username=config['kafka']['sasl.username'],
                               password=config['kafka']['sasl.password']
                           ))
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('topic', topic)
                   .start()
                   .awaitTermination()
                )

        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)



if __name__ == "__main__":
    # create a spark session
    spark_conn = SparkSession.builder.appName("SocketStreamingConsumer").getOrCreate()
    # start the streaming   
    start_streaming(spark_conn)