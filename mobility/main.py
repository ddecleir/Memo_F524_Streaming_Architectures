import multiprocessing
import Cassandra
import KafkaProducer
import subprocess

def kafka_producer_script():
    KafkaProducer.main()

def cassandra_script():
    Cassandra.main()

def flink_consumer_script():
    command1 = "../../flink/flink-1.16.0/bin/start-cluster.sh"
    command2 = "../../flink/flink-1.16.0/bin/flink run --python ./FlinkConsumer.py --jarfile ../flink-sql-connector-kafka-1.16.0.jar"

    # Lancer les deux commandes en parallèle
    subprocess.Popen(command1, shell=True).wait()
    subprocess.Popen(command2, shell=True)

if __name__ == '__main__':
    process1 = multiprocessing.Process(target=kafka_producer_script)
    process2 = multiprocessing.Process(target=cassandra_script)
    process3 = multiprocessing.Process(target=flink_consumer_script)

    process1.start()
    process2.start()
    process3.start()

    process1.join()
    process2.join()
    process3.join()

