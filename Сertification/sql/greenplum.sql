-- проверка кода для последующей вставки в DAG
create external table olejnikov_tv (
	session_id varchar(32),
	user_id int, 
	channel_name varchar(50),
	time_start timestamp,
	time_end timestamp
)
location('pxf://olejnikov.tv_dataset_mv?PROFILE=hive&server=hadoop')
format 'custom' (formatter='pxfwritable_import');

select * from olejnikov_tv limit 5;

drop external table olejnikov_tv;

-- проверяем таблицы
select * from olejnikov_tv_channels limit 5;

select * from olejnikov_tv_dataset limit 5;

select * from olejnikov_tv_stream limit 5;

-- создаем материализованное представление
create materialized view olejnikov_tv_mv as
with ts as (
	select session_id, user_id, channel_id, timer as time_start
	from (select * from olejnikov_tv_dataset where status = 'enabled') t1
	union
	select session_id, user_id, channel_id, timer as time_start
	from (select * from olejnikov_tv_stream where status = 'enabled') t2
), te as (
	select session_id, timer as time_end
	from (select * from olejnikov_tv_dataset where status = 'disabled') t3
	union
	select session_id, timer as time_end
	from (select * from olejnikov_tv_stream where status = 'disabled') t4
)
select ts.session_id, user_id, channel_name, time_start, time_end
from ts
join te using(session_id)
join olejnikov_tv_channels using(channel_id)
distributed randomly

drop materialized view olejnikov_tv_mv

select * from olejnikov_tv_mv limit 5

-- 	Рейтинг по времени просмотра
select channel_name, date_part('hour', sum(time_end - time_start)) as total_hours
from olejnikov_tv_mv
group by channel_name
order by total_hours desc

-- Рейтинг по количеству зрителей
select channel_name, count(channel_name) as count_views
from olejnikov_tv_mv
group by channel_name
order by count_views desc

-- Рейтинг каналов 
select channel_name, date_part('hour', sum(time_end - time_start)) as total_hours, count(channel_name) as count_views
from olejnikov_tv_mv
group by channel_name
order by total_hours desc

-- Количество просмотров по дням
select time_start::date as days, date_part('hour', sum(time_end - time_start)) as total_hours, count(*) as count_views
from olejnikov_tv_mv
group by days
order by days

-- Количество просмотров в разрезе каждого часа
select hours, count(hours) as count_views
from (
	select generate_series(date_part('hour', time_start::time)::int, date_part('hour', time_end::time)::int) as hours
	from olejnikov_tv_mv) h
group by hours
order by hours

-- дополнительное материализованное представление для распределения по часам
create materialized view olejnikov_tv_2_mv as
select hours, count(hours) as count_views
from (
	select generate_series(date_part('hour', time_start::time)::int, date_part('hour', time_end::time)::int) as hours
	from olejnikov_tv_mv) h
group by hours
order by hours

select * from olejnikov_tv_2_mv