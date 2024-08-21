import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, udf, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from config.config import config
from time import sleep
import openai
import os
import sys 

def sentiment_analysis(comment) -> str: # function to perform sentiment analysis
    if comment:  # check if the comment is not empty
        openai.api_key = config['openai']['api_key'] # set the OpenAI API key from the config file
        completion = openai.ChatCompletion.create( # create a chat completion object
            model='gpt-3.5-turbo', # specify the model to use
            messages = [ # create a list of messages
                {
                    "role": "system", # specify the role of the message and its content. "system" means the message is from the system
                    "content": """
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment:
                        
                        {comment}  
                    """.format(comment=comment) # insert the comment to be classified into the message
                }
            ]
        )
        return completion.choices[0].message['content'] # return the classification result
    return "Empty"  # return "Empty" if the comment is empty


def start_streaming(spark):
    topic = "yelp-reviews"
    while True: # loop to keep the stream running
        try:
            # read the data from the socket - this is a continuous stream of data
            # we call it socket since we are using the AF_INET socket to stream data
            stream_df = (spark.readStream.format("socket") # specify the format of the stream as socket stream 
                        .option("host", "127.0.0.1") # specify the host which in this case is the local machine
                        # but in general it is the IP address of the machine where the data is being streamed from (Source)
                        .option("port", 9999).load()) # end bracket is because we are writing in multiple lines

            # Define the schema for the data    
            # we should be knowing the schema of the data that is being streamed in order to parse it correctly. 
            # In this case, we know the schema of the data that is being streamed. 
            # if we don't know the schema of the data, we can use the inferSchema option to infer the schema of the data.

            # example of schema to infer is as below:

            # schema = stream_df.schema

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType()),
                StructField("feedback", StringType())
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


            # Send the data to OPENAI for sentiment analysis
            # Lets register the sentiment_analysis function as a UDF (User Defined Function) in Spark
            sentiment_analysis_udf = udf(sentiment_analysis, StringType()) # create a user-defined function for sentiment analysis and specify the return type

            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                             .otherwise(None)
                                             ) # create a new column in the DataFrame for the sentiment analysis results
                                            # when() function is used to check a condition and return a value if the condition is true
                                            # otherwise() function is used to return a value if the condition is false
            # Since we modified the stream_df DataFrame schema, we need to update the same in confluent Kafka platform.

            # example of the data that will be written to Kafka

            # { "review_id": "1", "user_id": "1", "business_id": "1", "stars": 5.0, "date": "2022-01-01", "text": "This is a great place", "feedback": "POSITIVE" }
            # { "review_id": "2", "user_id": "2", "business_id": "2", "stars": 1.0, "date": "2022-01-02", "text": "This is a terrible place", "feedback": "NEGATIVE" }

            # Dataframe will be written to Kafka in the above format
            # dataframe will internally convert to JSON format and then write to Kafka
            # the conversion of the dataframe to JSON format is done by the to_json() function

            # Part2 writing to Kafka
            # write the data to the Kafka topic

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            
            # selectExpr() function is used to select the columns from the DataFrame and apply an expression to them.
            # CAST() function is used to convert the column to the specified data type. Here review_id is converted to STRING data type.
            # to_json() function is used to convert the column to JSON format.
            # All the other columns will remain the same and will be converted to JSON format.
            # struct(*) is used to select all the columns from the DataFrame and convert them to JSON format. 
            # Key is the unique identifier for the data. In this case, review_id is used as the key. Primary key is used as the key.

            query = (kafka_df.writeStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                   # specify the Kafka bootstrap servers from the config file
                   #bootstrap servers are the list of servers that the producer will use to establish a connection to the Kafka cluster.
                   .option("kafka.security.protocol", config['kafka']['security.protocol'])
                   # specify the security protocol from the config file 
                   # security protocol is the protocol used to secure the communication between the producer and the Kafka cluster.
                   .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanism'])
                   # specify the SASL mechanism from the config file
                   # SASL mechanism is the mechanism used for authentication between the producer and the Kafka cluster.
                   .option('kafka.sasl.jaas.config',
                           'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                           'password="{password}";'.format(
                               username=config['kafka']['sasl.username'],
                               password=config['kafka']['sasl.password']
                           ))
                   .option('checkpointLocation', '/tmp/checkpoint')
                   # specify the checkpoint location where the offset information will be stored in case of failure
                   # offset here is the position of the data in the stream that has been processed by the consumer 
                   # Kafka uses the checkpoint location to store the offset information in case of failure so that the 
                   # we are writing spark streaming data to Kafka and this offset information is used to resume the stream
                   # from the last processed offset in case of failure or restart of the stream. 
                   .option('topic', topic)
                   .start() # start the stream. when we mean stream it start the stream and write the data to the Kafka topic
                   .awaitTermination() # keep the stream running until it is stopped by an exception or by invoking query.stop()
                )

        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)



if __name__ == "__main__":
    # create a spark session
    spark_conn = SparkSession.builder.appName("SocketStreamingConsumer").getOrCreate()
    # start the streaming   
    start_streaming(spark_conn)