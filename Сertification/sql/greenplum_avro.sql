create external table olejnikov_tv_avro (
	session_id varchar(32),
	user_id int, 
	channel_id int,
	time_start bigint,
	status varchar(8)
)
location('pxf:///user/olejnikov/tv_stream.avro?PROFILE=avro&server=hadoop')
format 'custom' (formatter='pxfwritable_import');

select * from olejnikov_tv_avro limit 5;

drop external table if exists olejnikov_tv_avro;

select session_id, user_id, channel_id,
       timestamp 'epoch' + time_start / 1000 * interval '1 second' as time_start,
       status
from olejnikov_tv_avro
limit 5