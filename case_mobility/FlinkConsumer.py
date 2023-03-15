from pyflink.table import EnvironmentSettings, StreamTableEnvironment, StatementSet
import numpy as np
from sklearn.linear_model import LogisticRegression
from pyflink.common.typeinfo import Types
from pyflink.table.types import DataTypes
from pyflink.table.sinks import CsvTableSink
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder
from pyflink.common.serialization import SimpleStringSchema
import requests

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

""" Consume a Kafka Topic """
#Retrieve transactions
deserialization_schema_transaction = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW_NAMED(
        ["id", "requestDate", "1m_t1_count", "1m_t1_speed", "1m_t1_occupancy", "1m_t1_start_time", "1m_t1_end_time", "5m_t1_count", "5m_t1_speed", "5m_t1_occupancy", "5m_t1_start_time", "5m_t1_end_time", "15m_t1_count", "15m_t1_speed", "15m_t1_occupancy", "15m_t1_start_time", "15m_t1_end_time", "60m_t1_count", "60m_t1_speed", "60m_t1_occupancy", "60m_t1_start_time", "60m_t1_end_time"]
        ,[Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])).build()
transport_stream = FlinkKafkaConsumer(
    topics='transport_stream',
    deserialization_schema=deserialization_schema_transaction,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})
ds_transport = env.add_source(transport_stream)

"""  Store the information capture in local """
output_path = 'mobility_output'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()


""" Send the data to Cassandra"""
serialization_schema_transaction = JsonRowSerializationSchema.builder().with_type_info(type_info=Types.ROW_NAMED(
        ["id", "requestDate", "1m_t1_count", "1m_t1_speed", "1m_t1_occupancy", "1m_t1_start_time", "1m_t1_end_time", "5m_t1_count", "5m_t1_speed", "5m_t1_occupancy", "5m_t1_start_time", "5m_t1_end_time", "15m_t1_count", "15m_t1_speed", "15m_t1_occupancy", "15m_t1_start_time", "15m_t1_end_time", "60m_t1_count", "60m_t1_speed", "60m_t1_occupancy", "60m_t1_start_time", "60m_t1_end_time"]
        ,[Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])).build()
kafka_producer = FlinkKafkaProducer(
    topic='output_stream',
    serialization_schema=serialization_schema_transaction,
    producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

def main():
    transport_data = table_env.from_data_stream(ds_transport)
    table_env.create_temporary_view("transport_data", transport_data)
    print(table_env.from_path("transport_data").print_schema())
    output_table = table_env.sql_query("SELECT * FROM transport_data")
    output = table_env.to_data_stream(output_table)
    output.sink_to(file_sink)
    output.add_sink(kafka_producer)
    env.execute()

if __name__ == '__main__':
    main()
