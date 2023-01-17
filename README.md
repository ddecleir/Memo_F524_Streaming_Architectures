# Memo_F524_Streaming_Architectures

**TransactionsGenerator.py** -- Transactions Generator - coming from the following book : https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html

**TransactionsTester.py** -- Code allowing to test the generator and to manage statistics and graphics. 

**SavingDatasetTransactions.py** -- Code to generate a file containing the generated transactions.

**KafkaMessageProducer.py** -- Generates a table containing the users, a table containing the terminals and then generates one transaction per second. These transactions as well as the data of the users and the terminals are sent thanks to a Kafka producer.

**FlinkConsumer.py** -- Uses the Pyflink library to retrieve the transactions sent by the Kafka producer (todo: create a processing of these data with a Data preprocessing, Feature engineering, Model selection and training, Model evaluation, Model fine-tuning). Moreover, the data which transit within this consumer are stored in a local file and sent thanks to another Kafka producer with the topic 'transaction_sink_topic' which will allow to store them in a Cassandra database.

**Cassandra.py** -- Retrieves data from the Kafka producer with the topic 'transaction_sink_topic' and stores it in a local Cassandra database. 


#To run the code :
Install the following librairies for Python Version 3.7.16 :

kafka : pip install kafka-python
numpy : pip install numpy
pandas : pip install pandas
matplotlib : pip install matplotlib
seaborn : pip install seaborn
apache-flink (pyflink) : pip install apache-flink
cassandra-driver : pip install cassandra-driver

Open 7 linux terminals an type the following commands.
1. Terminal Zookeeper : 
**../kafka_2.12-2.8.2/bin/zookeeper-server-start.sh ../kafka_2.12-2.8.2/config/zookeeper.properties**

2. Terminal Kafka : 
**../kafka_2.12-2.8.2/bin/kafka-server-start.sh ../kafka_2.12-2.8.2/config/server.properties**

3. Terminal Flink :  
**../flink/flink-1.16.0/bin/start-cluster.sh** 

4. Terminal Firefox : 
**firefox**

5. Terminal kafka sender :  
**python Thesis/KafkaMessageProducer.py**

6. Terminal Flink Consumer : 
**../flink/flink-1.16.0/bin/flink run --python Thesis/FlinkConsumer.py --jarfile Thesis/flink-sql-connector-kafka-1.16.0.jar**

7. Terminal Cassandra : 
**python Thesis/Cassandra.py**
