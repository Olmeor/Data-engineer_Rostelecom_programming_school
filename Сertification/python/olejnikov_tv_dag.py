from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator

DAG_NAME = 'olejnikov_tv_dag'
GP_CONN_ID = 'olejnikov_conn'

DATASET = '''
create external table olejnikov_tv_dataset (
	session_id varchar(32),
	user_id int, 
	channel_id int,
	timer timestamp,
	status varchar(8)
)
location('pxf://olejnikov.tv_dataset?PROFILE=hive&server=hadoop')
format 'custom' (formatter='pxfwritable_import');
'''

STREAM = '''
create external table olejnikov_tv_stream (
	session_id varchar(32),
	user_id int, 
	channel_id int,
	timer timestamp,
	status varchar(8)
)
location('pxf://olejnikov.tv_stream?PROFILE=hive&server=hadoop')
format 'custom' (formatter='pxfwritable_import');
'''

CHANNELS = '''
create external table olejnikov_tv_channels (
	channel_id int,
	channel_name varchar(50)
)
location('pxf://olejnikov.tv_channels?PROFILE=hive&server=hadoop')
format 'custom' (formatter='pxfwritable_import');
'''

args = {'owner': 'olejnikov',
        'start_date': days_ago(0),
        'depends_on_past': False}

with DAG(
    DAG_NAME, description='olejnikov_tv',
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1,
    default_args=args,
    params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:

    send_dataset = PostgresOperator(
        task_id='send_dataset',
        sql=DATASET,
        postgres_conn_id=GP_CONN_ID,
        autocommit=True)

    send_stream = PostgresOperator(
        task_id='send_stream',
        sql=STREAM,
        postgres_conn_id=GP_CONN_ID,
        autocommit=True)

    send_channels = PostgresOperator(
        task_id='send_to_channels',
        sql=CHANNELS,
        postgres_conn_id=GP_CONN_ID,
        autocommit=True)

send_dataset >> send_stream >> send_channels
