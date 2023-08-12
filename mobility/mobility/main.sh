#!/bin/bash

# Start a new tmux session in the background
tmux new-session -d -s my_session

# Starting Docker
tmux send-keys "echo 'Starting Docker...'" C-m
tmux send-keys "docker compose up -d" C-m
tmux send-keys "echo 'Starting Flink in 20 seconds...'" C-m


# Split the window into 3 vertical panes
tmux split-window -v
tmux split-window -v

# Run FlinkConsumer.py in the second pane
tmux select-pane -t 1
tmux send-keys "sleep 20" C-m
tmux send-keys "echo 'Running FlinkConsumer.py...'" C-m
tmux send-keys "docker exec -it flink-jobmanager ./bin/flink run -py /mobility/FlinkConsumer.py --jarfile /mobility/connector.jar" C-m

# Run Cassandra.py in the third pane
tmux select-pane -t 2
tmux send-keys "sleep 20" C-m
tmux send-keys "echo 'Running Cassandra.py...'" C-m
tmux send-keys "docker exec -it flink-jobmanager python /mobility/Cassandra.py" C-m

# Split each of the three vertical panes horizontally
tmux select-pane -t 0
tmux split-window -h

tmux select-pane -t 2
tmux split-window -h

tmux select-pane -t 4
tmux split-window -h

# Run KafkaProducer.py in the fourth pane
tmux select-pane -t 3
tmux send-keys "sleep 20" C-m
tmux send-keys "echo 'Running KafkaProducer.py...'" C-m
tmux send-keys "docker exec -it flink-jobmanager python /mobility/KafkaProducer.py" C-m

# Run ModelML.py in the fifth pane
tmux select-pane -t 5
tmux send-keys "sleep 30" C-m
tmux send-keys "echo 'Running Dashboard...'" C-m
tmux send-keys "docker exec -it flink-jobmanager python /mobility/Dashboard.py" C-m



# Attach to the tmux session
tmux attach -t my_session

# Function to stop services
stop_services() {
    # Stop Docker
    echo "Stopping Docker..."
    docker compose down
}

# Trap SIGINT (Ctrl+C) and SIGTSTP (Ctrl+Z) signals and call stop_services function
trap stop_services SIGINT SIGTSTP

# Keep script running to maintain services
while true
do
    sleep 1
done
