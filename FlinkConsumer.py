#Run with  ../../flink/flink-1.16.0/bin/flink run --python ./FlinkConsummer.py --jarfile ./flink-sql-connector-kafka-1.16.0.jar
"""
Part of the code are coming from : https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/datastream/intro_to_datastream_api/
"""
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder

""" Configuration """
# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
#env.add_jars("file://./flink-sql-connector-kafka.jar")

""" Consume a Kafka Topic """
deserialization_schema_transaction = JsonRowDeserializationSchema.builder().type_info(type_info=Types.ROW_NAMED(
        ["TRANSACTION_ID", "TX_DATETIME", "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT", "TX_TIME_SECONDS", "TX_TIME_DAYS", "TX_FRAUD", "TX_FRAUD_SCENARIO"]
        ,[Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.INT(), Types.INT(), Types.INT(), Types.INT()])).build()
transaction_consumer = FlinkKafkaConsumer(
    topics='transaction_topic',
    deserialization_schema=deserialization_schema_transaction,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})
ds = env.add_source(transaction_consumer)

"""  Store the information capture """

output_path = 'output'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()
ds.sink_to(file_sink)

# You can call the add_sink method to emit the data of a DataStream to a DataStream sink connector:

serialization_schema_transaction = JsonRowSerializationSchema.builder().with_type_info(type_info=Types.ROW_NAMED(
        ["TRANSACTION_ID", "TX_DATETIME", "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT", "TX_TIME_SECONDS", "TX_TIME_DAYS", "TX_FRAUD", "TX_FRAUD_SCENARIO"]
        ,[Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.INT(), Types.INT(), Types.INT(), Types.INT()])).build()

kafka_producer = FlinkKafkaProducer(
    topic='transaction_sink_topic',
    serialization_schema=serialization_schema_transaction,
    producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds.add_sink(kafka_producer)


""" Run the application """
# Execute the Flink job
env.execute()

