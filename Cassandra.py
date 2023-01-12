import os
import time

# Set up the Kafka consumer
from kafka import KafkaConsumer

transaction_consumer = KafkaConsumer(
    "transaction_sink_topic",
    bootstrap_servers=["localhost:9092"],
    group_id="test_group",
    value_deserializer=lambda x: x.decode("utf-8"),
)

# Set up the Cassandra connection
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import json

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

# Create the keyspace and table (if they don't already exist)
session.execute("CREATE KEYSPACE IF NOT EXISTS detectionfraud WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
session.execute("CREATE TABLE IF NOT EXISTS detectionfraud.transactions "
                "(transaction_id int PRIMARY KEY, tx_datetime text, customer_id int,"
                "terminal_id int, tx_amount double, tx_time_seconds int, tx_time_days int, tx_fraud int, tx_fraud_scenario int)")

# Prepare the insert statement
insert_transaction_stmt = SimpleStatement("INSERT INTO detectionfraud.transactions "
                                          "(transaction_id, tx_datetime, customer_id, terminal_id, tx_amount, tx_time_seconds, tx_time_days, tx_fraud, tx_fraud_scenario)"
                                          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", consistency_level=ConsistencyLevel.ONE)

# Consume messages from Kafka and insert them into Cassandra
for message in transaction_consumer:
    values = json.loads(message.value)
    transaction_id = values["TRANSACTION_ID"]
    tx_datetime = values["TX_DATETIME"]
    customer_id = values["CUSTOMER_ID"]
    terminal_id = values["TERMINAL_ID"]
    tx_amount = values["TX_AMOUNT"]
    tx_time_seconds = values["TX_TIME_SECONDS"]
    tx_time_days = values["TX_TIME_DAYS"]
    tx_fraud = values["TX_FRAUD"]
    tx_fraud_scenario = values["TX_FRAUD_SCENARIO"]

    session.execute(insert_transaction_stmt, (transaction_id, tx_datetime, customer_id, terminal_id, tx_amount, tx_time_seconds, tx_time_days, tx_fraud, tx_fraud_scenario))

    print(transaction_id, " ", tx_datetime , "added")

#session.execute("DROP TABLE IF EXISTS detectionfraud.transactions")