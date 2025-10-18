from kafka import KafkaConsumer
import json
import csv

# --- Configuration ---
BROKER_ZT_IP = '172.29.215.184'
TOPICS = ['topic-cpu', 'topic-mem']
CPU_CSV_FILE = 'cpu_data.csv'
MEM_CSV_FILE = 'mem_data.csv'

# --- Kafka Consumer Setup ---
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=f'{BROKER_ZT_IP}:9092',
    auto_offset_reset='earliest',
    group_id='consumer-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# --- Prepare CSV files ---
cpu_file = open(CPU_CSV_FILE, 'w', newline='')
cpu_writer = csv.writer(cpu_file)
cpu_writer.writerow(['ts', 'server_id', 'cpu_pct'])

mem_file = open(MEM_CSV_FILE, 'w', newline='')
mem_writer = csv.writer(mem_file)
mem_writer.writerow(['ts', 'server_id', 'mem_pct'])

print("Consumer 1 (CPU/Mem) is running and waiting for messages...")
try:
    for message in consumer:
        topic = message.topic
        data = message.value
        
        if topic == 'topic-cpu':
            cpu_writer.writerow([data['ts'], data['server_id'], data['cpu_pct']])
            cpu_file.flush()
            print(f"Stored CPU data: {data}")
        elif topic == 'topic-mem':
            mem_writer.writerow([data['ts'], data['server_id'], data['mem_pct']])
            mem_file.flush()
            print(f"Stored Memory data: {data}")

except KeyboardInterrupt:
    print("Stopping Consumer 1.")
finally:
    cpu_file.close()
    mem_file.close()
    consumer.close()