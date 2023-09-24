-- CREATE database olejnikov;

-- пользователи
create external table olejnikov.tv_users (
	user_id int,
	user_name varchar(50)
)
row format delimited fields terminated by ','
lines terminated by '\n'
tblproperties("skip.header.line.count"="1");

load data inpath '/user/olejnikov/tv_users.csv' overwrite into table olejnikov.tv_users;

select * from olejnikov.tv_users limit 5

-- каналы
create external table olejnikov.tv_channels (
	channel_id int,
	channel_name varchar(50)
)
row format delimited fields terminated by ','
lines terminated by '\n'
tblproperties("skip.header.line.count"="1");

load data inpath '/user/olejnikov/tv_channels.csv' overwrite into table olejnikov.tv_channels;

select * from olejnikov.tv_channels limit 5

-- датасет
create external table olejnikov.tv_dataset (
	session_id varchar(32),
	user_id int,
	channel_id int,
	timer timestamp,
	status varchar(8)
)
row format delimited fields terminated by ','
lines terminated by '\n'
tblproperties("skip.header.line.count"="1");

load data inpath '/user/olejnikov/tv_dataset.csv' overwrite into table olejnikov.tv_dataset;

select * from olejnikov.tv_dataset limit 5

-- стрим
create external table olejnikov.tv_stream (
	session_id varchar(32),
	user_id int,
	channel_id int,
	timer timestamp,
	status varchar(8)
)
row format delimited fields terminated by ','
lines terminated by '\n';

load data inpath '/user/olejnikov/tv_stream.csv' overwrite into table olejnikov.tv_stream;

select * from olejnikov.tv_dataset limit 5

-- материализованное представление (не поддерживает Spark, поддерживает GreenPlum)
create materialized view tv_dataset_mv as
with ts as (
	select session_id, user_id, channel_id, timer as time_start
	from (select * from tv_dataset where status = 'enabled') t1
	union
	select session_id, user_id, channel_id, timer as time_start
	from (select * from tv_stream where status = 'enabled') t2
), te as (
	select session_id, timer as time_end
	from (select * from tv_dataset where status = 'disabled') t3
	union
	select session_id, timer as time_end
	from (select * from tv_stream where status = 'disabled') t4
)
select ts.session_id, user_id, channel_name, time_start, time_end
from ts
join te on ts.session_id = te.session_id
join tv_channels on ts.channel_id = tv_channels.channel_id

select * from tv_dataset_mv limit 5

-- виртуальная таблица (не интегрируется в Greenplum, интегрируется в Spark)
create view tv_dataset_v as
with ts as (
	select session_id, user_id, channel_id, timer as time_start
	from (select * from tv_dataset where status = 'enabled') t1
	union
	select session_id, user_id, channel_id, timer as time_start
	from (select * from tv_stream where status = 'enabled') t2
), te as (
	select session_id, timer as time_end
	from (select * from tv_dataset where status = 'disabled') t3
	union
	select session_id, timer as time_end
	from (select * from tv_stream where status = 'disabled') t4
)
select ts.session_id, user_id, channel_name, time_start, time_end
from ts
join te on ts.session_id = te.session_id -- join te using(session_id)
join tv_channels on ts.channel_id = tv_channels.channel_id -- join tv_channels using(channel_id)

select * from tv_dataset_v limit 5

-- 	Рейтинг по времени просмотра в часах
select channel_name, round((sum(UNIX_TIMESTAMP(time_end) - UNIX_TIMESTAMP(time_start))) / 3600) sum_time
from tv_dataset_v
group by channel_name
order by sum_time desc

-- Рейтинг по количеству зрителей
select channel_name, count(channel_name) as count_views
from tv_dataset_v
group by channel_name
order by count_views desc

-- Рейтинг каналов
select channel_name, round((sum(UNIX_TIMESTAMP(time_end) - UNIX_TIMESTAMP(time_start))) / 3600) as sum_time, count(channel_name) as count_views
from tv_dataset_v
group by channel_name
order by sum_time desc

-- Количество просмотров по дням
select to_date(time_start) as time_dates, round((sum(UNIX_TIMESTAMP(time_end) - UNIX_TIMESTAMP(time_start))) / 3600) as sum_time, count(*) as count_views
from tv_dataset_v
group by to_date(time_start)
order by to_date(time_start)



