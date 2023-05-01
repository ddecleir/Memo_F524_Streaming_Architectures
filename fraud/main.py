import multiprocessing
import Cassandra
import KafkaProducer
import subprocess
import RealTimeAnalysis
import time

cassandra_table_name = "transactions"


def run_docker():
    command = "docker compose up -d"
    subprocess.Popen(command, shell=True)


def kafka_producer_script():
    KafkaProducer.main()


def cassandra_script():
    Cassandra.main(cassandra_table_name)


def flink_consumer_script():
    command1 = "../../flink/flink-1.16.0/bin/start-cluster.sh"
    command2 = "../../flink/flink-1.16.0/bin/flink run --python ./FlinkConsumer.py --jarfile ../flink-sql-connector-kafka-1.16.0.jar"

    # Lancer les deux commandes en parallèle
    subprocess.Popen(command1, shell=True).wait()
    subprocess.Popen(command2, shell=True)


def real_time_analysis_script():
    RealTimeAnalysis.main(cassandra_table_name)


if __name__ == '__main__':
    process_docker = multiprocessing.Process(target=run_docker)
    process_kafka = multiprocessing.Process(target=kafka_producer_script)
    process_cassandra = multiprocessing.Process(target=cassandra_script)
    process_flink = multiprocessing.Process(target=flink_consumer_script)
    process_real_time = multiprocessing.Process(target=real_time_analysis_script)

    process_docker.start()
    print("Docker started ...")
    time.sleep(30)
    print("Docker running ...")
    process_kafka.start()
    process_cassandra.start()
    process_flink.start()
    process_real_time.start()


