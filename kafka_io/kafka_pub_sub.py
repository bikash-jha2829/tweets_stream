import logging
import sys
from typing import List

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from utils.utils import load_config_yaml

conf = load_config_yaml()

config = {'bootstrap.servers': conf['kafka']['servers'], 'group.id': conf['kafka']['groupid'],
          'session.timeout.ms': conf['kafka']['timeout'],
          'auto.offset.reset': conf['kafka']['offset_reset']}


def create_topics(topics: List[str]):
    # Create kafka topics.

    new_topics = [NewTopic(topic=topic, num_partitions=2, replication_factor=1) for topic in topics]

    admin = AdminClient(config)

    fs = admin.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logging.info("Topic {} created".format(topic))
        except Exception as e:
            logging.error("Failed to create topic {}: {}".format(topic, e))


def publish(topic: str, key, value):
    # Publish to a  kafka topic
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))

    # producer1.produce(topic=kafka_topic_name, key=str(uuid4()), value=jsonv1, on_delivery=delivery_report)
    producer.produce(topic, value, callback=delivery_callback)

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
