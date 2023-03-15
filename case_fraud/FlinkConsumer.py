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

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

""" Consume a Kafka Topic """
#Retrieve transactions
deserialization_schema_transaction = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW_NAMED(
        ["TRANSACTION_ID", "TX_DATETIME", "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT", "TX_TIME_SECONDS", "TX_TIME_DAYS", "TX_FRAUD", "TX_FRAUD_SCENARIO"]
        ,[Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.INT(), Types.INT(), Types.INT(), Types.INT()])).build()
transactions_stream = FlinkKafkaConsumer(
    topics='transaction_topic',
    deserialization_schema=deserialization_schema_transaction,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})
ds_transactions = env.add_source(transactions_stream)

#Retrieve customers
deserialization_schema_customer = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW_NAMED(
    ["CUSTOMER_ID", "X_CUSTOMER_ID", "Y_CUSTOMER_ID", "MEAN_ACCOUNT", "STD_ACCOUNT", "MEAN_NB_TX_PER_DAY"],
    [Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()])).build()
customers_stream = FlinkKafkaConsumer(
    topics='customer_topic',
    deserialization_schema=deserialization_schema_customer,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})
ds_customers = env.add_source(customers_stream)

#Retrieve terminals
deserialization_schema_terminal = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW_NAMED(
    ["TERMINAL_ID", "X_TERMINAL_ID", "Y_TERMINAL_ID"],
    [Types.INT(), Types.FLOAT(), Types.FLOAT()])).build()
terminals_stream = FlinkKafkaConsumer(
    topics='terminal_topic',
    deserialization_schema=deserialization_schema_terminal,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})
ds_terminals = env.add_source(terminals_stream)


"""  Store the information capture in local """
output_path = 'fraud_output'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()

""" Send the data to Cassandra"""
serialization_schema_transaction = JsonRowSerializationSchema.builder().with_type_info(type_info=Types.ROW_NAMED(
        ["TRANSACTION_ID", "TX_DATETIME", "TX_AMOUNT", "TX_TIME_SECONDS", "TX_TIME_DAYS", "X_TERMINAL_ID", "Y_TERMINAL_ID", "X_CUSTOMER_ID", "Y_CUSTOMER_ID", "MEAN_ACCOUNT", "STD_ACCOUNT", "MEAN_NB_TX_PER_DAY", "TX_FRAUD"]
        ,[Types.INT(), Types.STRING(), Types.DOUBLE(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.INT()])).build()
kafka_producer = FlinkKafkaProducer(
    topic='output_topic',
    serialization_schema=serialization_schema_transaction,
    producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})


def main():
    terminals = table_env.from_data_stream(ds_terminals)
    customers = table_env.from_data_stream(ds_customers)
    transactions = table_env.from_data_stream(ds_transactions)

    table_env.create_temporary_view("terminals", terminals)
    table_env.create_temporary_view("customers", customers)
    table_env.create_temporary_view("transactions", transactions)

    print(table_env.from_path("terminals").print_schema())
    print(table_env.from_path("customers").print_schema())
    print(table_env.from_path("transactions").print_schema())

    output_table = table_env.sql_query("SELECT TRANSACTION_ID, TX_DATETIME, TX_AMOUNT, TX_TIME_SECONDS, TX_TIME_DAYS, "
                                       " X_TERMINAL_ID, Y_TERMINAL_ID, X_CUSTOMER_ID, "
                                       "Y_CUSTOMER_ID, MEAN_ACCOUNT, STD_ACCOUNT, MEAN_NB_TX_PER_DAY, TX_FRAUD FROM "
                                       "transactions t, terminals te, customers "
                                       "c WHERE t.TERMINAL_ID = te.TERMINAL_ID AND t.CUSTOMER_ID = c.CUSTOMER_ID")
    output = table_env.to_data_stream(output_table)
    output.sink_to(file_sink)
    output.add_sink(kafka_producer)
    output.print()
    env.execute()
if __name__ == '__main__':
    main()