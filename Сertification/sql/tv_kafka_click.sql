-- интеграция Клика с Кафка очередью
create table destudy.olejnikov_kafka
(
  session_id String,
  user_id UInt16,
  channel_id UInt16,
  timer DateTime,
  status String
)
ENGINE = Kafka()
SETTINGS
   kafka_broker_list = 'vm-strmng-s-1.test.local:9092',
   kafka_topic_list = 'olejnikov_topic_ch',
   kafka_group_name = 'olejnikov_group',
   kafka_format = 'JSONEachRow';

create table destudy.olejnikov_data
(
  session_id String,
  user_id UInt16,
  channel_id UInt16,
  timer DateTime,
  status String
)
ENGINE = MergeTree()
ORDER BY (timer);
  
create materialized view destudy.olejnikov_kafka_mv to destudy.olejnikov_data as
select *
from destudy.olejnikov_kafka;

select * from destudy.olejnikov_kafka_mv limit 5
  