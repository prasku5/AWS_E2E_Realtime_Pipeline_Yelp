# Realtime Data Streaming With TCP Socket, Apache Spark, OpenAI LLM, Kafka and Elasticsearch 


## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)

## Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline using TCP/IP Socket, Apache Spark, OpenAI LLM, Kafka and Elasticsearch. It covers each stage from data acquisition, processing, sentiment analysis with ChatGPT, production to kafka topic and connection to elasticsearch.

## System Architecture
![System_architecture.png](assets%2FSystem_architecture.png)

The project is designed with the following components:

- **Data Source**: We use `yelp.com` dataset for our pipeline.
- **TCP/IP Socket**: Used to stream data over the network in chunks
- **Apache Spark**: For data processing with its master and worker nodes.
- **Confluent Kafka**: Our cluster on the cloud
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Kafka Connect**: For connecting to elasticsearch
- **Elasticsearch**: For indexing and querying

## What's achieved 

- Setting up data pipeline with TCP/IP 
- Conducted Real-time data streaming with Apache Kafka
- Applied Data processing techniques with Apache Spark
- Performed Realtime sentiment analysis with OpenAI ChatGPT
- Synchronised data from kafka to elasticsearch
- Indexed and Querying data on elasticsearch

## Technologies

- Python
- TCP/IP
- Confluent Kafka
- Apache Spark
- Docker
- Elasticsearch

Credits to Yusuf Ganiyu( https://github.com/airscholar ) for his contribution to make this project possible. 


## Overall architecture in text flow 

+----------------------------+
|   Read JSON File           |
| from File System           |
+----------------------------+
                  |
                  v
+----------------------------+
|  Create Socket and Bind    |
|  to Host and Port          |
+----------------------------+
                  |
                  v
+----------------------------+
| Stream Data over Socket    |
+----------------------------+
                  |
                  v
+----------------------------+         +----------------------------+
| Read Data from Socket      |         |      Setup Spark Session   |
| (Spark Streaming)          +<--------+----------------------------+
+----------------------------+
                  |
                  v
+----------------------------+
|    Parse JSON Data         |
+----------------------------+
                  |
                  v
+----------------------------+
| Perform Sentiment Analysis |
| using OpenAI API           |
+----------------------------+
                  |
                  v
+----------------------------+
| Update DataFrame with      |
| sentiment results          |
+----------------------------+
                  |
                  v
+----------------------------+
| Write processed data to    |
| Kafka                      |
+----------------------------+
                  |
                  v
+----------------------------+
| Handle exceptions          |
+----------------------------+
                  |
                  v
+----------------------------+
| Wait and retry             |
+----------------------------+
                  |
                  v
+----------------------------+
|           End              |
+----------------------------+