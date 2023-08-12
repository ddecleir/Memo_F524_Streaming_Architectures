from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import json
import time

def main(tableName):
    # Set up the Kafka consumer
    kafka_server = ["broker:29092"]
    transaction_consumer = KafkaConsumer(
        "output_topic",
        bootstrap_servers=kafka_server,
        group_id="test_group",
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    print(transaction_consumer)

    # Set up the Cassandra connection
    cluster = Cluster(["cassandra"])
    session = cluster.connect()
    print(session)
    # Create the keyspace and table (if they don't already exist)
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS detectionfraud WITH REPLICATION = {'class': 'SimpleStrategy', "
        "'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS detectionfraud." + tableName + " " +
                    "(transaction_id int PRIMARY KEY, tx_datetime text, tx_amount double,"
                    "tx_time_seconds int, tx_time_days int, x_terminal_id float, y_terminal_id float, x_customer_id "
                    "float, y_customer_id float, mean_account float, std_account float, mean_nb_tx_per_day float, "
                    "tx_fraud int)")

    # Prepare the insert statement
    insert_transaction_stmt = SimpleStatement("INSERT INTO detectionfraud." + tableName + " " +
                                              "(transaction_id, tx_datetime, tx_amount, tx_time_seconds, "
                                              "tx_time_days, x_terminal_id, y_terminal_id, x_customer_id, "
                                              "y_customer_id, mean_account, std_account, mean_nb_tx_per_day, tx_fraud) "
                                              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)",
                                                consistency_level=ConsistencyLevel.LOCAL_ONE)

    transaction_count = 0
    for message in transaction_consumer:
        values = json.loads(message.value)
        transaction_id = values["TRANSACTION_ID"]
        tx_datetime = values["TX_DATETIME"]
        tx_amount = values["TX_AMOUNT"]
        tx_time_seconds = values["TX_TIME_SECONDS"]
        tx_time_days = values["TX_TIME_DAYS"]
        x_terminal_id = values["X_TERMINAL_ID"]
        y_terminal_id = values["Y_TERMINAL_ID"]
        x_customer_id = values["X_CUSTOMER_ID"]
        y_customer_id = values["Y_CUSTOMER_ID"]
        mean_account = values["MEAN_ACCOUNT"]
        std_account = values["STD_ACCOUNT"]
        mean_nb_tx_per_day = values["MEAN_NB_TX_PER_DAY"]
        tx_fraud = values["TX_FRAUD"]

        session.execute(insert_transaction_stmt, (
            transaction_id, tx_datetime, tx_amount, tx_time_seconds, tx_time_days, x_terminal_id, y_terminal_id,
            x_customer_id, y_customer_id, mean_account, std_account, mean_nb_tx_per_day, tx_fraud))

        transaction_count += 1
        print("Inserted transaction " + str(transaction_count) + " at " + str(time.time()))
        with open('logs.txt', 'a') as f:
            f.write(f"Inserted {transaction_count} at {time.time()}\n")

if __name__ == "__main__":
    tableName = "transactions"
    main(tableName)
