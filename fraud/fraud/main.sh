#!/bin/bash

# Function to start services
start_services() {
    # Start a new tmux session in the background named "services"
    tmux new-session -d -s services

    # Starting Docker
    tmux send-keys "echo 'Starting Docker...'" C-m
    tmux send-keys "docker compose up -d" C-m

    # Split the window into 2 vertical panes. One for the left (50% width) and the other for the right (50% width)
    tmux split-window -h -p 50

    # In the left pane, split it horizontally into two
    tmux select-pane -t 0
    tmux split-window -v -p 50

    # In the right pane, split it horizontally into three
    tmux select-pane -t 2
    tmux split-window -v -p 66
    tmux split-window -v -p 50

    # Running FlinkConsumer.py
    tmux select-pane -t 0
    tmux send-keys "sleep 20" C-m
    tmux send-keys "echo 'Running FlinkConsumer.py...'" C-m
    tmux send-keys "docker exec -it flink-jobmanager ./bin/flink run -py /fraud/FlinkConsumer.py --jarfile /fraud/connector.jar" C-m

    # Running Cassandra.py
    tmux select-pane -t 1
    tmux send-keys "sleep 30" C-m
    tmux send-keys "echo 'Running Cassandra.py...'" C-m
    tmux send-keys "docker exec -it flink-jobmanager python /fraud/Cassandra.py" C-m

    # Running KafkaProducer.py
    tmux select-pane -t 2
    tmux send-keys "sleep 30" C-m
    tmux send-keys "echo 'Running KafkaProducer.py...'" C-m
    tmux send-keys "docker exec -it flink-jobmanager python /fraud/KafkaProducer.py" C-m

    # Running ModelML.py after waiting 120 seconds
    tmux select-pane -t 3
    tmux send-keys "echo 'Waiting 30 seconds for services to start up...'" C-m
    tmux send-keys "sleep 45" C-m
    tmux send-keys "echo 'Starting Model creation...'" C-m
    tmux send-keys "docker exec -it flink-jobmanager python /fraud/ModelML.py" C-m

    # Running Dashboard
    tmux select-pane -t 4
    tmux send-keys "sleep 60" C-m
    tmux send-keys "echo 'Running Dashboard...'" C-m
    tmux send-keys "docker exec -it flink-jobmanager python /fraud/Dashboard.py" C-m

    # Attach to the tmux session
    tmux attach -t services
}

# Function to stop services
stop_services() {
    # Stop Docker
    echo "Stopping Docker..."
    docker compose down

    # Deactivate python environment (if applicable, remove if not necessary)
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
