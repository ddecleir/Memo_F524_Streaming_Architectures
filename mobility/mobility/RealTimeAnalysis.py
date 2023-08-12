import json
import pickle
import pandas as pd
import numpy as np
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import ModelML


class RealTimeAnalysis:
    counter = 0

    def __init__(self):
        self.cluster = Cluster(["cassandra"])
        self.session = self.cluster.connect()
        self.session.set_keyspace('brusselmobility')
        self.session.execute(
            "DROP TABLE IF EXISTS mobility_prediction;"
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS mobility_prediction (
                transaction_id text,
                prediction_time timestamp,
                prediction float,
                PRIMARY KEY (transaction_id, prediction_time)
            );
            """
        )

    def insert_prediction_in_cassandra(self, prediction, transaction_id):
        current_time = datetime.now()
        self.session.execute(
            "INSERT INTO mobility_prediction (transaction_id, prediction_time, prediction) VALUES (%s, %s, %s)",
            (transaction_id, current_time, prediction)
        )

    def transform_data(self, data):
        # Convert data to DataFrame
        df = pd.DataFrame(data)

        column_mapping = {
            'id': 'ID',
            'requestDate': 'REQUEST_DATE',
            '1m_t1_count': 'T1_M1_COUNT',
            '1m_t1_speed': 'T1_M1_SPEED',
            '1m_t1_occupancy': 'T1_M1_OCCUPANCY',
            '1m_t1_start_time': 'T1_M1_START_TIME',
            '1m_t1_end_time': 'T1_M1_END_TIME',
            '5m_t1_count': 'T1_5M_COUNT',
            '5m_t1_speed': 'T1_5M_SPEED',
            '5m_t1_occupancy': 'T1_5M_OCCUPANCY',
            '5m_t1_start_time': 'T1_5M_START_TIME',
            '5m_t1_end_time': 'T1_5M_END_TIME',
            '15m_t1_count': 'T1_15M_COUNT',
            '15m_t1_speed': 'T1_15M_SPEED',
            '15m_t1_occupancy': 'T1_15M_OCCUPANCY',
            '15m_t1_start_time': 'T1_15M_START_TIME',
            '15m_t1_end_time': 'T1_15M_END_TIME',
            '60m_t1_count': 'T1_60M_COUNT',
            '60m_t1_speed': 'T1_60M_SPEED',
            '60m_t1_occupancy': 'T1_60M_OCCUPANCY',
            '60m_t1_start_time': 'T1_60M_START_TIME',
            '60m_t1_end_time': 'T1_60M_END_TIME'
        }

        df.rename(columns=column_mapping, inplace=True)

        print("code : Data transformed:")
        print(df)

        # Convert dates
        date_columns = ["REQUEST_DATE", "T1_M1_START_TIME", "T1_M1_END_TIME", "T1_5M_START_TIME",
                        "T1_5M_END_TIME", "T1_15M_START_TIME", "T1_15M_END_TIME", "T1_60M_START_TIME",
                        "T1_60M_END_TIME"]

        for col in date_columns:
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].map(pd.Timestamp.to_julian_date)

        print(df)
        # Handle missing values
        df = df.replace('-', np.nan)
        df = df.dropna()

        # Load the scaler and transform the features
        with open("scaler.pkl", "rb") as scaler_file:
            encoder = pickle.load(scaler_file)

        if len(df) > 0:
            df.iloc[:, 1:-1] = encoder.transform(df.iloc[:, 1:-1])
            return df
        return None


    def run_real_time_analysis(self):
        print("code : Run real time analysis")
        model = ModelML.import_model()
        # Inside the run_real_time_analysis function, before prediction
        kafka_server = ["broker:29092"]
        transaction_consumer = KafkaConsumer(
            "prediction_topic",
            bootstrap_servers=kafka_server,
            group_id="test_group",
            value_deserializer=lambda x: json.loads(x),
        )

        for message in transaction_consumer:
            # Filter data with the specific ID 'MON_TD1'
            if message.value['id'] == 'MON_TD1':
                print(message.value)
                data = self.transform_data([message.value])
                print("code : Data transformed:")
                print(data)
                if data is None:
                    continue
                # Drop ID and target column for prediction
                X = data.drop(columns=['ID', 'T1_60M_END_TIME'])

                prediction = model.predict(X)
                transaction_id = data.iloc[0]["ID"]


                print(f"Prediction: {prediction[0]} for transaction {transaction_id}")
                self.insert_prediction_in_cassandra(float(prediction[0]), transaction_id)
