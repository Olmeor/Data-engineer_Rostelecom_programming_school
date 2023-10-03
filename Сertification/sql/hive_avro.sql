CREATE TABLE olejnikov.tv_avro
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    TBLPROPERTIES ('avro.schema.literal'='
{
    "doc": "olejnikov_avro_schema",
    "name": "tv_stream",
    "namespace": "tv_stream",
    "type": "record",
    "fields": [
        {"name": "session_id", "type": "string"},
        {"name": "user_id", "type": "int"},
        {"name": "channel_id", "type": "int"},
        {"name": "timer", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "status", "type": "string"}
    ]
}')

DESCRIBE olejnikov.tv_avro

LOAD DATA INPATH '/user/olejnikov/tv_stream.avro' OVERWRITE INTO TABLE olejnikov.tv_avro;

SELECT * FROM olejnikov.tv_avro LIMIT 5;

