import pandas as pd
from kafka import KafkaProducer
import json
import time

# --- Configuration ---
DATASET_PATH = 'dataset.csv'
BROKER_ZT_IP = '172.29.215.184'

# --- Kafka Producer Setup ---
producer = KafkaProducer(
    bootstrap_servers=f'{BROKER_ZT_IP}:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Read and Stream Data ---
try:
    print("Reading dataset...")
    df = pd.read_csv(DATASET_PATH)
    print("Dataset loaded. Starting to stream metrics...")

    for index, row in df.iterrows():
        # Prepare data for each topic
        cpu_data = {'ts': row['ts'], 'server_id': row['server_id'], 'cpu_pct': row['cpu_pct']}
        mem_data = {'ts': row['ts'], 'server_id': row['server_id'], 'mem_pct': row['mem_pct']}
        net_data = {'ts': row['ts'], 'server_id': row['server_id'], 'net_in': row['net_in']}
        disk_data = {'ts': row['ts'], 'server_id': row['server_id'], 'disk_io': row['disk_io']}

        # Send data to respective topics
        producer.send('topic-cpu', value=cpu_data)
        producer.send('topic-mem', value=mem_data)
        producer.send('topic-net', value=net_data)
        producer.send('topic-disk', value=disk_data)

        print(f"Sent data for timestamp: {row['ts']}")
        
        # Simulate real-time stream with a small delay
        time.sleep(0.1) 

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Ensure all messages are sent before exiting
    producer.flush()
    producer.close()
    print("Producer has finished sending all data.")
