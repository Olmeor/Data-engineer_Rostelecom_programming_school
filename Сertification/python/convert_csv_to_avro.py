from fastavro import writer
import pandas as pd
from datetime import datetime
import numpy as np

schema_dataset = {
    'doc': 'dataset_avro_schema',
    'name': 'dataset',
    'namespace': 'dataset',
    'type': 'record',
    'fields': [
        {'name': 'session_id', 'type': 'string'},
        {'name': 'user_id', 'type': 'int'},
        {'name': 'channel_id', 'type': 'int'},
        {'name': 'timer', 'type': {'type': 'long', 'logicalType': 'timestamp-millis'}},
        {'name': 'status', 'type': 'string'},
    ]
}

schema_channels = {
    'doc': 'channels_avro_schema',
    'name': 'channels',
    'namespace': 'channels',
    'type': 'record',
    'fields': [
        {'name': 'channel_id', 'type': 'int'},
        {'name': 'channel_name', 'type': 'string'},
    ]
}

schema_users = {
    'doc': 'users_avro_schema',
    'name': 'users',
    'namespace': 'users',
    'type': 'record',
    'fields': [
        {'name': 'user_id', 'type': 'int'},
        {'name': 'user_name', 'type': 'string'},
    ]
}

df_dataset = pd.read_csv('../dataset/tv_dataset.csv')
df_channels = pd.read_csv('../dataset/tv_channels.csv')
df_users = pd.read_csv('../dataset/tv_users.csv')

df_dataset['timer'] = pd.to_datetime(df_dataset['timer'], format='%Y-%m-%d %H:%M:%S')

with (open('../dataset/tv_dataset.avro', 'wb') as dataset,
      open('../dataset/tv_channels.avro', 'wb') as channels,
      open('../dataset/tv_users.avro', 'wb') as users):

    writer(dataset, schema_dataset, df_dataset.to_dict('records'))
    writer(channels, schema_channels, df_channels.to_dict('records'))
    writer(users, schema_users, df_users.to_dict('records'))

print('Done')

print(df_dataset.head(5))
