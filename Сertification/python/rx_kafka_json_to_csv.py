from kafka import KafkaConsumer
import json
import pandas as pd
from hdfs import InsecureClient

topic = 'olejnikov_topic'
hdfs_host = 'http://vm-dlake2-m-1:9870'
hdfs_path = '/user/olejnikov'
# header_dataset = ['session_id', 'user_id', 'channel_id', 'timer', 'status']
message_count = 10000  # users from tx
client = InsecureClient(hdfs_host, user='olejnikov')

consumer = KafkaConsumer(topic,
                         bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         group_id='olejnikov_group'
                         )

rows = []  # [header_dataset]

try:
    i = 0
    for message in consumer:
        # print(f"New messgae: {message.value.decode('utf-8)')}")
        data = json.loads(message.value.decode('utf-8)'))
        rows.append(list(data.values()))
        if i == message_count * 2 - 1:
            df = pd.DataFrame(rows)
            with client.write(hdfs_path + '/tv_stream.csv', encoding='utf-8') as tv_stream:
                df.to_csv(tv_stream, header=None, index=False)
                print('Done')
        i += 1

finally:
    consumer.close()
