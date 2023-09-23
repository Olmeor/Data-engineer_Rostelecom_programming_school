from kafka import KafkaProducer
import json
import random
from datetime import datetime, date, time as dt, timedelta


producer = KafkaProducer(
    bootstrap_servers='vm-strmng-s-1.test.local:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8')
)


def generate_record(session_id, status):
    if status == 'disabled':
        timer = queue[session_id][2]
        delta = timedelta(hours=random.randint(0, 3), minutes=random.randint(0, 59))
        rec = [session_id, queue[session_id][0], queue[session_id][1], str(timer + delta), status]
        queue.pop(session_id)
    else:
        user_id = random.randint(1, users)
        channel_id = random.randint(1, channels)
        per = random.randint(1, 3)
        if per == 1:
            timer = datetime.combine(date.today(), dt(random.randint(3, 20), random.randint(0, 59), random.randint(0, 59)))
        else:
            timer = datetime.combine(date.today(), dt(19, random.randint(0, 59), random.randint(0, 59)))
        rec = [session_id, user_id, channel_id, str(timer), status]
        queue[session_id] = [user_id, channel_id, timer]

    record = {
        "session_id": rec[0],
        "user_id": rec[1],
        "channel_id": rec[2],
        "timer": rec[3],
        "status": rec[4]
    }

    return record


queue = {}
users = 10000
channels = 148
# header_queue = ['session_id', 'user_id', 'channel_id', 'timer', 'status']

try:
    topic = 'olejnikov_topic'
    i = 0
    while i < users:  # True
        session = "%032x" % random.getrandbits(128)
        data = generate_record(session, 'enabled')
        producer.send(topic, key=data['session_id'], value=data)
        data = generate_record(session, 'disabled')
        producer.send(topic, key=data['session_id'], value=data)
        print(f'User {data["user_id"]} channel {data["channel_id"]} is generated')
        i += 1  # time.sleep(0.1)

finally:
    producer.close()
