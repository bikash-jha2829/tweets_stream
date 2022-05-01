import json
import logging
import sys
from pprint import pprint
from typing import Callable

from confluent_kafka import Consumer, KafkaException

from experimental.elastic import run

config = {'bootstrap.servers': "127.0.0.1:9200", 'group.id': "test",
          'auto.offset.reset': "latest"}


def subscribe(topics, function: Callable = None):
    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(config)

    def print_assignment(consumer, partitions):
        logger.info('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=2.0)
            if msg is None:
                logger.info("waiting for message to arrive")
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                # Extract the (optional) key and value, and print.
                logger.info(f"Consumed event from topic {msg.topic()}: key={msg.key().decode('utf-8')}, value:")
                v = msg.value().decode("utf-8")
                pprint(json.loads(v))
                print("")
                if function:
                    function(v)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()


if __name__ == '__main__':
    subscribe("tweets", run)
