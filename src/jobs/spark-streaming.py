import pyspark
from pyspark.sql import SparkSession

def start_streaming(spark):
    # read the data from the socket
    stream_df = (spark.readStream.format("socket")
                .option("host", "localhost")
                .option("port", 9999).load()) # end bracket is because we are writing in multiple lines

    query = (stream_df.writeStream.outputMode("append").format("console").start())
    query.awaitTermination() # this is to keep the stream running



if __name__ == "__main__":
    # create a spark session
    spark_conn = SparkSession.builder.appName("SocketStreamingConsumer").getOrCreate()
    # start the streaming
    start_streaming(spark_conn)