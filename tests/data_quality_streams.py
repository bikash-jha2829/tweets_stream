from faker import Faker
from time import sleep, time
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
fake = Faker()

def get_registered_user():
    return {
        "name": fake.name(),
        "address": fake.address(),
        "phone": fake.phone_number(),
        "job": fake.job(),
        "email": fake.email(),
        "dob": fake.date(),
        "created_at": fake.year()
    }

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))
    while 1 == 1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("custchannel", registered_user)
        time.sleep(10)
    consumer = KafkaConsumer(
        'custchannel',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='custdq',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    def loadtotable():
        pass




