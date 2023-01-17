# Memo_F524_Streaming_Architectures

**TransactionsGenerator.py** -- Transactions Generator - coming from the following book : https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html

**TransactionsTester.py** -- Code allowing to test the generator and to manage statistics and graphics. 

**SavingDatasetTransactions.py** -- Code to generate a file containing the generated transactions.

**KafkaMessageProducer.py** -- Generates a table containing the users, a table containing the terminals and then generates one transaction per second. These transactions as well as the data of the users and the terminals are sent thanks to a Kafka producer.

**FlinkConsumer.py** -- Uses the Pyflink library to retrieve the transactions sent by the Kafka producer (todo: create a processing of these data with a Data preprocessing, Feature engineering, Model selection and training, Model evaluation, Model fine-tuning). Moreover, the data which transit within this consumer are stored in a local file and sent thanks to another Kafka producer with the topic 'transaction_sink_topic' which will allow to store them in a Cassandra database.

**Cassandra.py** -- Retrieves data from the Kafka producer with the topic 'transaction_sink_topic' and stores it in a local Cassandra database. 
