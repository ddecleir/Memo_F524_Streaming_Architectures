
import pandas as pd
from kafka import KafkaProducer
import requests
import time
import json

def serialize_message(data):
    return json.dumps(data).encode('utf-8')

def get_stream_real_time(url, producer):
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            return line

def main():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'])
    url = 'https://data.mobility.brussels/traffic/api/counts/?request=live'

    previous = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. Suspendisse lectus tortor"
    start = time.time()
    while True:
        time.sleep(20)
        message = get_stream_real_time(url, producer)
        if message[47:] != previous[47:]:
            previous = message
            message = message.decode('utf-8')
            message = str(serialize_message(message))
            message = message.replace('\\\\', '')
            message = message[3:-2]
            df = pd.read_json(message)
            new_df = pd.DataFrame()
            for index, row in df.iterrows():
                # Get the data dictionary for the current row
                data_dict = row["data"]["results"]
                split_data = {}
                # Loop through each time interval ("1m", "5m", "15m", "60m")
                for time_interval in data_dict.keys():
                    # Loop through each traffic sensor ("t1", "t2")
                    for sensor in data_dict[time_interval].keys():
                        if (sensor == "t1"):
                            # Create column names for the count, speed, and occupancy values
                            count_col = f"{time_interval}_{sensor}_count"
                            speed_col = f"{time_interval}_{sensor}_speed"
                            occupancy_col = f"{time_interval}_{sensor}_occupancy"
                            start_time_col = f"{time_interval}_{sensor}_start_time"
                            end_time_col = f"{time_interval}_{sensor}_end_time"
                            # Get the count, speed, and occupancy values for the current sensor and time interval
                            count = data_dict[time_interval][sensor]["count"]
                            speed = data_dict[time_interval][sensor]["speed"]
                            occupancy = data_dict[time_interval][sensor]["occupancy"]
                            start_time = data_dict[time_interval][sensor]["start_time"]
                            end_time = data_dict[time_interval][sensor]["end_time"]
                            # Create a dictionary with the split data for the current row
                            split_data[count_col] = count
                            split_data[speed_col] = speed
                            split_data[occupancy_col] = occupancy
                            split_data[start_time_col] = start_time
                            split_data[end_time_col] = end_time
                # print(index, split_data)
                temp_df = pd.DataFrame(split_data, index=[index])
                new_df = pd.concat([new_df, temp_df], axis=0)
            # Concatenate the new DataFrame with the original DataFrame
            df = pd.concat([df, new_df], axis=1)
            # Drop the original "data" column
            df = df.drop("data", axis=1)
            df.reset_index(inplace=True)
            df = df.rename(columns={'index': 'id'})
            for i in range(len(df) - 1):
                data_to_send = {
                    "id": str(df["id"][i]),
                    "requestDate": str(df["requestDate"][i]),
                    "1m_t1_count": str(df["1m_t1_count"][i]),
                    "1m_t1_speed": str(df["1m_t1_speed"][i]),
                    "1m_t1_occupancy": str(df["1m_t1_occupancy"][i]),
                    "1m_t1_start_time": str(df["1m_t1_start_time"][i]),
                    "1m_t1_end_time": str(df["1m_t1_end_time"][i]),
                    "5m_t1_count": str(df["5m_t1_count"][i]),
                    "5m_t1_speed": str(df["5m_t1_speed"][i]),
                    "5m_t1_occupancy": str(df["5m_t1_occupancy"][i]),
                    "5m_t1_start_time": str(df["5m_t1_start_time"][i]),
                    "5m_t1_end_time": str(df["5m_t1_end_time"][i]),
                    "15m_t1_count": str(df["15m_t1_count"][i]),
                    "15m_t1_speed": str(df["15m_t1_speed"][i]),
                    "15m_t1_occupancy": str(df["15m_t1_occupancy"][i]),
                    "15m_t1_start_time": str(df["15m_t1_start_time"][i]),
                    "15m_t1_end_time": str(df["15m_t1_end_time"][i]),
                    "60m_t1_count": str(df["60m_t1_count"][i]),
                    "60m_t1_speed": str(df["60m_t1_speed"][i]),
                    "60m_t1_occupancy": str(df["60m_t1_occupancy"][i]),
                    "60m_t1_start_time": str(df["60m_t1_start_time"][i]),
                    "60m_t1_end_time": str(df["60m_t1_end_time"][i]),
                }
                producer.send('transport_stream', serialize_message(data_to_send))
            end = time.time()
            print(end - start)
            start = time.time()

if __name__ == '__main__':
    main()
