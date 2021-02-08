import json
import logging
import os
import aiofiles
from timeseries_replay.publishers.BasePublisher import BasePublisher
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

class KafkaPublisher(BasePublisher):

    def __init__(self, bootstrap_servers, topic):
        
        logging.info('Initiating kafka Publisher')
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        
    def publish(self, obj, batch_name):
        for dictionary in obj:
            result = json.dumps(dictionary)

            self.producer.poll(0)
            self.producer.produce(self.topic, result.encode('utf-8'), callback=self.delivery_report)
        self.producer.flush()
