# Streaming Architectures for Machine Learning and Data Analytics in Python

This project is a comprehensive study and implementation of streaming architectures tailored for machine learning and data analytics using Python. The project showcases the integration of several technologies, including Kafka, Flink, and Cassandra, to simulate and process transaction data in real-time.

## Getting Started

Before you can use the provided Docker images, you need to ensure Docker is installed on your system.

### Docker Images

To simplify the setup process, we've containerized the entire environment using Docker. You'll need to pull the following Docker images:

- **Fraud Detection**: [fraud-flink-python](https://hub.docker.com/repository/docker/ddecleir/fraud-flink-python/general)
- **Mobility**: [mobility-flink-python](https://hub.docker.com/repository/docker/ddecleir/mobility-flink-python/general)

### Running the Project

Once you've pulled the Docker images:

1. For the **Fraud Detection** module, run:
   ```bash
   fraud/main.sh
   ```
2. For the Mobility module, run:
   ```bash
   mobility/main.sh
   ```

### Requirements
The necessary Python libraries are already integrated into the Docker images. However, for reference, here's the list:

makefile
Copy code
kafka-python==2.0.2
numpy==1.21.6
pandas==1.3.5
matplotlib==3.3.4
seaborn==0.11.1
apache-flink==1.17.1
cassandra-driver==3.25.0
scikit-learn==1.3.0
pyflink==1.0
flask==2.3.2

### Additional Tools
It's recommended to install tmux for a better experience while working with multiple terminal windows. This allows for simultaneous monitoring and management of different components of the project.
