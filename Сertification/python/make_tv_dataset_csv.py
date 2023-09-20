import csv
import random
import datetime
import math
from faker import Faker
import time


def make_record(session, user, channel, hour, minute):
    time_start = datetime.datetime.combine(day, datetime.time(hour, random.randint(0, 59), random.randint(0, 59)))
    roll = random.randint(1, 5)
    if time_start.weekday() < 5 and roll == 1:
        return 
    time_end = time_start + datetime.timedelta(minutes=minute, seconds=random.randint(0, 59))
    writer_dataset.writerow([session, user, channel, time_start, 'enabled'])
    writer_dataset.writerow([session, user, channel, time_end, 'disabled'])


start = time.time()
header_dataset = ['session_id', 'user_id', 'channel_id', 'timer', 'status']
header_channels = ['channel_id', 'channel_name']
header_users = ['user_id', 'user_name']
days = 7
user_id = 0

with (open('../tv_rating.csv', 'r', encoding='utf-8') as file_in,
      open('../tv_dataset.csv', 'w', encoding='utf-8', newline='') as dataset,
      open('../tv_channels.csv', 'w', encoding='utf-8', newline='') as channels,
      open('../tv_users.csv', 'w', encoding='utf-8', newline='') as users):
    data = file_in.readlines()
    writer_dataset = csv.writer(dataset)
    writer_dataset.writerow(header_dataset)
    writer_channels = csv.writer(channels)
    writer_channels.writerow(header_channels)
    writer_users = csv.writer(users)
    writer_users.writerow(header_users)

    for d in range(days):
        day = datetime.date.today() - datetime.timedelta(days=days) + datetime.timedelta(days=d)
        for c in range(1, len(data)):
            print('day:', d + 1, 'channel:', c)
            row = data[c].split(',')
            users = int(float(row[3]) * 100)
            minutes = int(float(row[2])) * 60 + int((float(row[2]) - int(float(row[2]))) * 60 / 100)
            if d == 0:
                writer_channels.writerow([int(row[0]), row[1]])

            for i in range(user_id + 1, user_id + users + 1):
                sess = "%032x" % random.getrandbits(128)
                if d == 0:
                    writer_users.writerow([i, Faker('ru_RU').name()])
                per = random.randint(1, 5)
                if per == 1:
                    make_record(sess, i, int(row[0]), random.randint(0, 23 - math.ceil(minutes / 60)), minutes)
                else:
                    make_record(sess, i, int(row[0]), 20 - round(minutes / 120), minutes)
            user_id += users
    user_id = 0

end = time.time() - start
print(end)
