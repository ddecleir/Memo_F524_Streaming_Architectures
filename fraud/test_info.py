import time
import random
import math

def simulate_process(num_records, filename):
    with open(filename, 'w') as f:
        for i in range(1, num_records + 1):  # Starting from 1 to avoid division by zero in the delay formula
            # Simulate the time it takes to send a message (Kafka)
            sent_time = time.time()
            f.write(f'Sent message {i} at {sent_time}\n')

            # Calculate delay (Cassandra)
            # The delay increases non-linearly (logarithmically) with i
            # Adding random.uniform to simulate varying conditions in a real-world scenario
            delay = math.log(i) * random.uniform(0.0001, 0.001)

            # Simulate the time it takes to insert a message
            inserted_time = time.time() + delay
            f.write(f'Inserted {i} at {inserted_time}\n')

# Simulate processes with different numbers of records
simulate_process(1000, 'logs_1000.txt')
simulate_process(10000, 'logs_10000.txt')
simulate_process(20000, 'logs_20000.txt')
simulate_process(50000, 'logs_50000.txt')
