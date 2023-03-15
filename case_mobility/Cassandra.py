import os
import time

from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import json

def main():
    # Set up the Kafka consumer
    kafka_server = ["localhost:9092"]

    transport_stream = KafkaConsumer(
        "output_stream",
        bootstrap_servers=kafka_server,
        group_id="test_group",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    # Set up the Cassandra connection
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # session.execute("DROP TABLE IF EXISTS brusselmobility.public_transport")
    # session.execute("DROP KEYSPACE IF EXISTS brusselmobility")

    # Create the keyspace and table (if they don't already exist)
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS brusselmobility WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS brusselmobility.data_input "
                    "(key_id timeuuid PRIMARY KEY, id text, requestDate text, t1_m1_count text, t1_m1_speed text, t1_m1_occupancy text, "
                    "t1_m1_start_time text, t1_m1_end_time text, t1_5m_count text, t1_5m_speed text, t1_5m_occupancy "
                    "text, t1_5m_start_time text, t1_5m_end_time text, t1_15m_count text, t1_15m_speed text, "
                    "t1_15m_occupancy text, t1_15m_start_time text, t1_15m_end_time text, t1_60m_count text, t1_60m_speed "
                    "text, t1_60m_occupancy text, t1_60m_start_time text, t1_60m_end_time text)")
    # Prepare the insert statement
    insert_transport_stmt = SimpleStatement("INSERT INTO brusselmobility.data_input "
                                            "(key_id, id, requestDate, t1_m1_count, t1_m1_speed, t1_m1_occupancy, "
                                            "t1_m1_start_time, t1_m1_end_time, t1_5m_count, t1_5m_speed, t1_5m_occupancy, "
                                            "t1_5m_start_time, t1_5m_end_time, t1_15m_count, "
                                            "t1_15m_speed, t1_15m_occupancy, t1_15m_start_time, t1_15m_end_time, "
                                            "t1_60m_count, t1_60m_speed, t1_60m_occupancy, t1_60m_start_time, "
                                            "t1_60m_end_time) "
                                            "VALUES (now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                            "%s, %s, %s, %s, %s, %s)", consistency_level=ConsistencyLevel.ONE)

    # Consume messages from Kafka and insert them into Cassandra
    for message in transport_stream:
        values = json.loads(message.value)
        id = str(values["id"])
        requestDate = str(values["requestDate"])
        t1_1m_count = str(values["1m_t1_count"])
        t1_1m_speed = str(values["1m_t1_speed"])
        t1_1m_occupancy = str(values["1m_t1_occupancy"])
        t1_1m_start_time = str(values["1m_t1_start_time"])
        t1_1m_end_time = str(values["1m_t1_end_time"])
        t1_5m_count = str(values["5m_t1_count"])
        t1_5m_speed = str(values["5m_t1_speed"])
        t1_5m_occupancy = str(values["5m_t1_occupancy"])
        t1_5m_start_time = str(values["5m_t1_start_time"])
        t1_5m_end_time = str(values["5m_t1_end_time"])
        t1_15m_count = str(values["15m_t1_count"])
        t1_15m_speed = str(values["15m_t1_speed"])
        t1_15m_occupancy = str(values["15m_t1_occupancy"])
        t1_15m_start_time = str(values["15m_t1_start_time"])
        t1_15m_end_time = str(values["15m_t1_end_time"])
        t1_60m_count = str(values["60m_t1_count"])
        t1_60m_speed = str(values["60m_t1_speed"])
        t1_60m_occupancy = str(values["60m_t1_occupancy"])
        t1_60m_start_time = str(values["60m_t1_start_time"])
        t1_60m_end_time = str(values["60m_t1_end_time"])

        session.execute(insert_transport_stmt, (
            id, requestDate, t1_1m_count, t1_1m_speed, t1_1m_occupancy, t1_1m_start_time, t1_1m_end_time, t1_5m_count,
            t1_5m_speed, t1_5m_occupancy, t1_5m_start_time, t1_5m_end_time, t1_15m_count, t1_15m_speed,
            t1_15m_occupancy,
            t1_15m_start_time, t1_15m_end_time, t1_60m_count, t1_60m_speed, t1_60m_occupancy, t1_60m_start_time,
            t1_60m_end_time))
        print(id, " ", requestDate, "added")

if __name__ == '__main__':
    main()