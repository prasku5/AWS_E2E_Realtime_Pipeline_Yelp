import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def start_streaming(spark):
    try:
        # read the data from the socket
        stream_df = (spark.readStream.format("socket")
                    .option("host", "localhost")
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
        
        # Since the socket stream is a continuous stream of data, we need to parse the data into a DataFrame.
        # currently stream_df is a DataFrame with a single column named value. we need to parse the data into a DataFrame with the specified schema.
        stream_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
        # The from_json() function is used to parse a JSON string and convert it into a DataFrame with the specified schema.
        # The select() function is used to select the columns from the DataFrame.

        query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start() # This is just writing to console.
        # console is the sink where the data will be written to.
        # In this case, the data will be written to the console.
        query.awaitTermination() # this is to keep the stream running
        # awaitTermination() is a method that waits for the termination of the query.
        # This method will block until the query is stopped either by an exception or by invoking query.stop().
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    # create a spark session
    spark_conn = SparkSession.builder.appName("SocketStreamingConsumer").getOrCreate()
    # start the streaming
    start_streaming(spark_conn)