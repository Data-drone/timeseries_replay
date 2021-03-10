import pytest
from confluent_kafka import Consumer
from timeseries_replay.publishers.kafka_publisher import KafkaPublisher
import logging
import time

logger = logging.getLogger(__name__)

@pytest.fixture
def test_list():
    return [{'timestamp': '2020-01-01 10:00:00', 'test1': 'A', 'testB': 0 },
        {'timestamp': '2020-01-01 10:00:01', 'test1': 'B', 'testB': 1 },
        {'timestamp': '2020-01-01 10:00:02', 'test1': 'C', 'testB': 2 },
        {'timestamp': '2020-01-01 10:00:03', 'test1': 'D', 'testB': 3 },
        {'timestamp': '2020-01-01 10:00:04', 'test1': 'E', 'testB': 4 },
        {'timestamp': '2020-01-01 10:00:05', 'test1': 'F', 'testB': 5 },
        {'timestamp': '2020-01-01 10:00:06', 'test1': 'G', 'testB': 6 },
        {'timestamp': '2020-01-01 10:00:07', 'test1': 'H', 'testB': 7 }
        ]

test_pairs = [(2,2), (4,4)]
# TODO do we need to have bootstrap server and topic?
@pytest.mark.parametrize("batch,target", test_pairs)
def test_tumbling_window_batcher(test_list, batch, target, bootstrap_servers='kafka:9092', topic='test_stream'):
    """Test the tumbling window batcher under edge conditions
    
    Scenarios:
        - Perfect fit ie list length 10 and windows of 2 - PASS

    """

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )

    counter = 0
    for i in publisher._tumbling_window_batcher(obj=test_list, batch_size=batch):
        assert len(i) == target
        counter += 1
    
    assert counter == len(test_list)/batch

    publisher.close()


def test_large_batch(test_list, bootstrap_servers='kafka:9092', topic='test_stream'):
    """Test the tumbling window batcher under edge conditions
    
    Scenarios:
        - batch_size above length - PASS    

    """

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )

    counter = 0
    for i in publisher._tumbling_window_batcher(obj=test_list, batch_size=12):
        assert len(i) == 8
        counter += 1
    
    # why is this equals to 2?
    assert counter == 1

    publisher.close()


def test_irregular_batch(test_list, bootstrap_servers='kafka:9092', topic='test_stream'):
    """Test the tumbling window batcher under edge conditions
    
    Scenarios:
        - batch_size and window don't separate evenly    

    """

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )

    counter = 0
    for i in publisher._tumbling_window_batcher(obj=test_list, batch_size=3):
        if len(i) == 3:
            pass
        elif len(i) == 2:
            pass
        else:
            assert 0
        counter +=1
    
    assert counter == 3

    publisher.close()

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

    publisher.close()
    
    

