"""
The code use to generate the transactions is coming from the following source :  https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html
"""

# Import the necessary modules
from kafka import KafkaProducer
import json
import time
from TransactionsGenerator import *


"""
Let us generate a dataset that features
5000 customers
10000 terminals
183 days of transactions (which corresponds to a simulated period from 2018/04/01 to 2018/09/30)
"""
(customer_profiles_table, terminal_profiles_table, transactions_df)=\
    generate_dataset(n_customers = 500 ,n_terminals = 100 ,nb_days=183 ,start_date="2018-04-01" ,r=5)
# Lets add some transactions :
Transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def serialize_message(data):
    return json.dumps(data).encode('utf-8')

# Generate a continuous stream of data
customer_size = customer_profiles_table["CUSTOMER_ID"].size
for index_customer in range(customer_size):
    customer_id = customer_profiles_table["CUSTOMER_ID"][index_customer]
    x_customer_id = customer_profiles_table["x_customer_id"][index_customer]
    y_customer_id = customer_profiles_table["y_customer_id"][index_customer]
    mean_amount = customer_profiles_table["mean_amount"][index_customer]
    std_amount = customer_profiles_table["std_amount"][index_customer]
    mean_nb_tx_per_day = customer_profiles_table["mean_nb_tx_per_day"][index_customer]
    customer_data = {"CUSTOMER_ID": int(customer_id),
                    "X_CUSTOMER_ID": x_customer_id,
                    "Y_CUSTOMER_ID": y_customer_id,
                    "MEAN_ACCOUNT" : mean_amount,
                    "STD_ACCOUNT": std_amount,
                    "MEAN_NB_TX_PER_DAY": mean_nb_tx_per_day}
    # Send the message to the 'customer_topic'
    producer.send('customer_topic', serialize_message(customer_data))
    print(customer_data)

terminal_size = terminal_profiles_table["TERMINAL_ID"].size
for index_terminal in range(terminal_size):
    terminal_id = terminal_profiles_table["TERMINAL_ID"][index_terminal]
    x_terminal_id = terminal_profiles_table["x_terminal_id"][index_terminal]
    y_terminal_id = terminal_profiles_table["y_terminal_id"][index_terminal]
    terminal_data = {"TERMINAL_ID": int(terminal_id),
                     "X_TERMINAL_ID": x_terminal_id,
                     "Y_TERMINAL_ID": y_terminal_id}
    # Send the message to the 'terminal_topic'
    producer.send('terminal_topic', serialize_message(terminal_data))
    print(terminal_data)

transactions_size = Transactions_df.size
for index_transaction in range(transactions_size):
    transaction_id = Transactions_df["TRANSACTION_ID"][index_transaction]
    tx_datetime = Transactions_df["TX_DATETIME"][index_transaction]
    customer_id = Transactions_df["CUSTOMER_ID"][index_transaction]
    terminal_id = Transactions_df["TERMINAL_ID"][index_transaction]
    tx_amount = Transactions_df["TX_AMOUNT"][index_transaction]
    tx_time_seconds = Transactions_df["TX_TIME_SECONDS"][index_transaction]
    tx_time_days = Transactions_df["TX_TIME_DAYS"][index_transaction]
    tx_fraud = Transactions_df["TX_FRAUD"][index_transaction]
    tx_fraud_scenario = Transactions_df["TX_FRAUD_SCENARIO"][index_transaction]
    transaction_data = {"TRANSACTION_ID": int(transaction_id),
                        "TX_DATETIME": str(tx_datetime),
                        "CUSTOMER_ID": int(customer_id),
                        "TERMINAL_ID": int(terminal_id),
                        "TX_AMOUNT": tx_amount,
                        "TX_TIME_SECONDS": int(tx_time_seconds),
                        "TX_TIME_DAYS": int(tx_time_days),
                        "TX_FRAUD": int(tx_fraud),
                        "TX_FRAUD_SCENARIO": int(tx_fraud_scenario)}
    # Send the message to the 'transaction_topic'
    producer.send('transaction_topic', serialize_message(transaction_data))
    time.sleep(1)
    print(transaction_data)

producer.close()