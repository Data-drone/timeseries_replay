import pytest
from confluent_kafka import Consumer
from timeseries_replay.publishers.kafka_publisher import KafkaPublisher
import logging
import time

logger = logging.getLogger(__name__)

def test_kafka_producer(caplog, dataset, bootstrap_servers='kafka:9092', topic='test_stream'):
    """Test whether the records are being published
    """

    caplog.set_level(logging.INFO)

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )

    test = [{'timestamp': '2020-01-01 10:00:00', 'test1': 'A', 'testB': 0 },
            {'timestamp': '2020-01-01 10:00:01', 'test1': 'B', 'testB': 1 },
            {'timestamp': '2020-01-01 10:00:02', 'test1': 'C', 'testB': 2 },
            {'timestamp': '2020-01-01 10:00:03', 'test1': 'D', 'testB': 3 }
            ]

    publisher.publish(test, 'testing_set')

    logger.info('starting consumer')


    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'testing',
        'auto.offset.reset': 'earliest'
    })

    # read back the message
    c.subscribe([topic])

    logger.info('starting consumer polling')

    run = True
    while run:
        msg = c.poll(5.0)
    
        if msg is None:
            c.close()
            run = False
            logger.info('Consumer returned NoneType')
            break
        if msg.error():
            logger.warn("Consumer error: {}".format(msg.error()))
            continue

        elif msg.value() in test:
            logger.info('message {0} is in the testing set'.format(msg) )
        else:
            logger.error('message {0} not in test set'.format(msg)) 


        logger.info('Received message: {}'.format(msg.value().decode('utf-8')))
    
    

