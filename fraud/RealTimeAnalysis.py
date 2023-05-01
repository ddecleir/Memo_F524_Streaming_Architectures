import os
import time

from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import json
import ModelML
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import pickle


class RealTimeAnalysis:
    counter = 0
    fraud_counter = 0
    def __init__(self):
        self.scaler = None
        self.cluster = Cluster(["127.0.0.1"])
        self.session = self.cluster.connect()
        self.session.set_keyspace('detectionfraud')
        self.session.execute(
            "CREATE TABLE IF NOT EXISTS transaction_prediction (transaction_id int PRIMARY KEY, prediction "
            "text);")

    @classmethod
    def get_counter(cls):
        counter_str = "Number of transactions: " + str(cls.counter) + " Number of frauds: " + str(cls.fraud_counter)
        if cls.fraud_counter != 0:
            counter_str = str(counter_str) + " Percentage of frauds: " + str(cls.fraud_counter / cls.counter)
        return counter_str

    def insert_prediction_in_cassandra(self, prediction, transaction_id):
        self.session.execute("INSERT INTO transaction_prediction (transaction_id, prediction) VALUES (%s, %s)",
                             (transaction_id, prediction))

    def transform_data(self, data):
        data = pd.DataFrame(np.array(data))
        data[1] = pd.to_datetime(data[1])
        data[1] = data[1].map(pd.Timestamp.to_julian_date)
        data.columns = ['TRANSACTION_ID', 'TX_DATETIME', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS',
                        'X_TERMINAL_ID', 'Y_TERMINAL_ID', 'X_CUSTOMER_ID', 'Y_CUSTOMER_ID', 'MEAN_ACCOUNT',
                        'STD_ACCOUNT', 'MEAN_NB_TX_PER_DAY', 'TX_FRAUD']
        # I need to scale the data to have a better accuracy
        # I will use the StandardScaler function from sklearn to scale the data except the last column and the first column
        data.iloc[:, 1:-1] = self.scaler.transform(data.iloc[:, 1:-1])
        # remove the first column which only contains an ID for each transaction
        data = data.iloc[:, 1:]
        # remove the fraud column
        data = data.iloc[:, :-1]
        return data

    def run_real_time_analysis(self):
        print("REAL TIME ANALYSIS STARTED")
        self.real_time_analysis()
        print("REAL TIME ANALYSIS FINISHED")

    def real_time_analysis(self):
        # Retrieve model
        model = ModelML.import_model()

        """ Consumes messages from a Kafka topic and inserts them into a Cassandra table. """

        # Set up the Kafka consumer
        kafka_server = ["localhost:9092"]

        transaction_consumer = KafkaConsumer(
            "prediction_topic",
            bootstrap_servers=kafka_server,
            group_id="test_group",
            value_deserializer=lambda x: x.decode("utf-8"),
        )

        # Consume messages from Kafka and insert them into Cassandra
        with open("scaler.pkl", "rb") as scaler_file:
            self.scaler = pickle.load(scaler_file)
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

            data = [
                [transaction_id, tx_datetime, tx_amount, tx_time_seconds, tx_time_days, x_terminal_id, y_terminal_id,
                 x_customer_id, y_customer_id, mean_account, std_account, mean_nb_tx_per_day, tx_fraud]]
            data = self.transform_data(data)
            # I will use the model to predict the fraud
            prediction = ModelML.make_a_prediction(model, data)
            RealTimeAnalysis.counter += 1
            if prediction[0] == 1:
                RealTimeAnalysis.fraud_counter += 1
            # I will insert the prediction in the Cassandra table
            print("Prediction: " + str(prediction[0]) + " for transaction " + str(transaction_id))
            self.insert_prediction_in_cassandra(str(prediction[0]), transaction_id)

