#!/bin/bash

# Path to project directory
PROJECT_DIR="~/Documents/thesis/code"

# Function to start services
start_services() {
    echo "Starting services..."

    cd "$PROJECT_DIR"

    # Starting Docker
    echo "Starting Docker..."
    docker compose up -d

    # Wait for Docker to start up
    sleep 10

    # Starting Flink Cluster
    echo "Starting Flink Cluster..."
    ./flink/bin/start-cluster.sh

    # Wait for Flink Cluster to start up
    sleep 10

    # Running FlinkConsumer.py in a new terminal
    echo "Running FlinkConsumer.py..."
    gnome-terminal -- bash -c "flink/bin/flink run -pyclientexec /usr/bin/python3 -pyexec /usr/bin/python3 -py fraud/FlinkConsumer.py --jarfile connector.jar; exec bash"

    # Running Cassandra.py in a new terminal
    echo "Running Cassandra.py..."
    source ./venv/bin/activate
    gnome-terminal -- bash -c "python $PROJECT_DIR/fraud/Cassandra.py; exec bash"

    # Running KafkaProducer.py in a new terminal
    echo "Running KafkaProducer.py..."
    gnome-terminal -- bash -c "python $PROJECT_DIR/fraud/KafkaProducer.py; exec bash"

    # Activate python environment and run RealTimeAnalysis.py in a new terminal
    echo "Running RealTimeAnalysis.py..."
    cd $PROJECT_DIR/fraud
    gnome-terminal -- bash -c "cd fraud/; python $PROJECT_DIR/fraud/Dashboard.py; exec bash"
}

# Function to stop services
stop_services() {
    echo "Stopping services..."

    cd "$PROJECT_DIR"

    # Stop the Flink Cluster
    echo "Stopping Flink Cluster..."
    ./flink/bin/stop-cluster.sh

    # Stop Docker
    echo "Stopping Docker..."
    docker compose down

    # Deactivate python environment
    deactivate
}

# Trap SIGINT (Ctrl+C) and SIGTSTP (Ctrl+Z) signals and call stop_services function
trap stop_services SIGINT SIGTSTP

# Start services
start_services

# Keep script running to maintain services
while true
do
    sleep 1
done

