from kafka import KafkaConsumer
import json
import pandas as pd
from hdfs import InsecureClient
from fastavro import writer
import subprocess

topic = 'olejnikov_topic'
hdfs_host = 'http://vm-dlake2-m-1:9870'
hdfs_path = '/user/olejnikov'
header_dataset = ['session_id', 'user_id', 'channel_id', 'timer', 'status']
message_count = 10000  # users from tx
client = InsecureClient(hdfs_host, user='olejnikov')

consumer = KafkaConsumer(topic,
                         bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         group_id='olejnikov_group'
                         )

rows = []  # [header_dataset]

schema = {
    'doc': 'olejnikov_avro_schema',
    'name': 'tv_stream',
    'namespace': 'tv_stream',
    'type': 'record',
    'fields': [
        {'name': 'session_id', 'type': 'string'},
        {'name': 'user_id', 'type': 'int'},
        {'name': 'channel_id', 'type': 'int'},
        {'name': 'timer', 'type': {'type': 'long', 'logicalType': 'timestamp-millis'}},
        {'name': 'status', 'type': 'string'},
    ]
}

try:
    i = 0
    for message in consumer:
        # print(f"New messgae: {message.value.decode('utf-8)')}")
        data = json.loads(message.value.decode('utf-8)'))
        rows.append(list(data.values()))
        if i == message_count * 2 - 1:
            df = pd.DataFrame(rows)
            with client.write(hdfs_path + '/tv_stream.csv', encoding='utf-8') as tv_stream_csv:
                df.to_csv(tv_stream_csv, header=None, index=False)
            df.columns = header_dataset
            df['timer'] = pd.to_datetime(df['timer'], format='%Y-%m-%d %H:%M:%S')
            with open('/home/olejnikov/tv_stream.avro', 'wb') as tv_stream_avro:
                writer(tv_stream_avro, schema, df.to_dict('records'))
            subprocess.call([f'hdfs dfs -put /home/olejnikov/tv_stream.avro {hdfs_path}'], shell=True)
            subprocess.call(['rm /home/olejnikov/tv_stream.avro'], shell=True)
            print('Done')
        i += 1

finally:
    consumer.close()
