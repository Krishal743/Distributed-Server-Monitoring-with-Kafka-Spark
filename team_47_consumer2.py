from kafka import KafkaConsumer
import json
import csv

# --- Configuration ---
BROKER_ZT_IP = '172.29.215.184'
TOPICS = ['topic-net', 'topic-disk']
NET_CSV_FILE = 'net_data.csv'
DISK_CSV_FILE = 'disk_data.csv'

# --- Kafka Consumer Setup ---
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=f'{BROKER_ZT_IP}:9092',
    auto_offset_reset='earliest',
    group_id='consumer-group-2',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# --- Prepare CSV files ---
net_file = open(NET_CSV_FILE, 'w', newline='')
net_writer = csv.writer(net_file)
net_writer.writerow(['ts', 'server_id', 'net_in'])

disk_file = open(DISK_CSV_FILE, 'w', newline='')
disk_writer = csv.writer(disk_file)
disk_writer.writerow(['ts', 'server_id', 'disk_io'])

print("Consumer 2 (Net/Disk) is running and waiting for messages...")
try:
    for message in consumer:
        topic = message.topic
        data = message.value
        
        if topic == 'topic-net':
            net_writer.writerow([data['ts'], data['server_id'], data['net_in']])
            net_file.flush()
            print(f"Stored Network data: {data}")
        elif topic == 'topic-disk':
            disk_writer.writerow([data['ts'], data['server_id'], data['disk_io']])
            disk_file.flush()
            print(f"Stored Disk data: {data}")

except KeyboardInterrupt:
    print("Stopping Consumer 2.")
finally:
    net_file.close()
    disk_file.close()
    consumer.close()