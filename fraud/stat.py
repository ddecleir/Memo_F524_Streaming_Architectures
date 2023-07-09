import re

# List of log file names
log_files = ['logs_10.txt', 'logs_100.txt', 'logs_1000.txt', 'logs_10000.txt', 'logs_20000.txt', 'logs_50000.txt']

# Initialize lists to store the results
throughputs = []
avg_latencies = []

# Process each log file
for log_file in log_files:
    # Read the contents of the log file into a string
    with open(log_file, 'r') as file:
        log_data = file.read()

    # Find all the timestamps in the log file using regular expressions
    timestamps = re.findall(r'\d+\.\d+', log_data)

    # Convert the timestamps to floats
    timestamps = [float(timestamp) for timestamp in timestamps]

    # Calculate the throughput by dividing the number of messages by the total time taken
    num_records = len(timestamps) // 2
    total_time = timestamps[-1] - timestamps[0]
    throughput = num_records / total_time
    throughputs.append(throughput)

    # Calculate the average latency
    latencies = []
    for i in range(num_records):
        sent_time = timestamps[i * 2]
        insert_time = timestamps[i * 2 + 1]
        latency = insert_time - sent_time
        latencies.append(latency)

    avg_latency = sum(latencies) / num_records
    avg_latencies.append(avg_latency)

# Generate LaTeX table
table = "\\begin{table}[h]\n"
table += "\t\\centering\n"
table += "\t\\caption{Log File Results}\n"
table += "\t\\begin{tabular}{|c|c|c|}\n"
table += "\t\t\\hline\n"
table += "\t\t\\textbf{Log File} & \\textbf{Throughput (records per second)} & \\textbf{Average Latency (seconds)} \\\\\n"
table += "\t\t\\hline\n"

for i in range(len(log_files)):
    table += f"\t\t{log_files[i]} & {throughputs[i]:.2f} & {avg_latencies[i]:.5f} \\\\\n"
    table += "\t\t\\hline\n"

table += "\t\\end{tabular}\n"
table += "\\end{table}"

# Print the LaTeX table
print(table)
