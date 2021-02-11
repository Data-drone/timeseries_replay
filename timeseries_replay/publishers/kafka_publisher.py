"""Kafka Publisher

Publisher that writes to a kafka topic leveraging the confluent_kafka library

"""
import json
import logging
import os
import aiofiles
import datetime
from timeseries_replay.publishers.BasePublisher import BasePublisher
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

class KafkaPublisher(BasePublisher):
    """Kafka Publisher

    Initialises a kafka producer

    #TODO convert to async publisher
    
    Args:
        bootstrap_servers(str): a string of kafka brokers in the format <kafka_broker>:<kafka_port>
        topic(str): Kafka topic to write to we assume that this has been created already
    """

    def __init__(self, bootstrap_servers, topic):
        
        logging.info('Initiating kafka Publisher')
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    def _delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger.debug('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        
    def json_cleaner(self, item):
        if isinstance(item, datetime.datetime):
            return item.__str__()

    def publish(self, obj, batch_name):
        """Publish Command

        Args:
            obj(list(dict)): a list of dicts to be published tuple by tuple
            batch_name(str): Does nothing in this case

        """
        for dictionary in obj:
            result = json.dumps(dictionary, default=self.json_cleaner)

            self.producer.poll(0)
            self.producer.produce(self.topic, result.encode('utf-8'), callback=self._delivery_report)
        self.producer.flush()
