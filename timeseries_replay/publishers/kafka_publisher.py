"""Kafka Publisher

Publisher that writes to a kafka topic leveraging the confluent_kafka library

"""
import orjson as json
#import json
import logging
import datetime
from timeseries_replay.publishers.BasePublisher import BasePublisher
from confluent_kafka import Producer, KafkaException
import asyncio
from threading import Thread
import numpy as np
from itertools import islice

logger = logging.getLogger(__name__)

class KafkaPublisher(BasePublisher):
    """Kafka Publisher

    Initialises a kafka producer

    #TODO convert to async publisher
        - Need to start the loop in this class so that we don't need to rewrite the whole thing for async
        - We can start the loop here but we will need to trigger a close to the polling at somestage
    
    Args:
        bootstrap_servers(str): a string of kafka brokers in the format <kafka_broker>:<kafka_port>
        topic(str): Kafka topic to write to we assume that this has been created already
    """

    def __init__(self, bootstrap_servers, topic):
        
        logging.info('Initiating kafka Publisher')
        self.producer = Producer({'bootstrap.servers': bootstrap_servers,
                                    'linger.ms': 500})
        self.topic = topic

        # async event loop
        self._loop = asyncio.new_event_loop()
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self.producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def _delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
        else:
            logger.debug('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        
    def json_cleaner(self, item):
        """Json Cleaner function

        Function to make sure we can json dumps datetime properly
        encodes objects of datetime to string. If we wanted to submit other objs would need to
        add to this function

        Args:
            item (obj): item to be encoded for json dumps

        """
        if isinstance(item, datetime.datetime):
            return item.__str__()

    def _tumbling_window_batcher(self, obj, batch_size):
        """Tumbling Window Batcher

        Batches up individual dicts into tumpling windows.
        ie for batch_size 2, [0,1,2,3,4,5,6] would produce
        [0,1], [2,3], [4,5], [6] 

        Args:
            obj (list(dict)): a list of dict objects
            batch_size (int): the size of batch that we should expect

        """

        iter_item_list = np.arange(0, len(obj),batch_size)
        iter_item_list = np.append(iter_item_list, len(obj))

        # case if batch size and length of object divide perfectly
        for i in range(0, len(iter_item_list)-1):
            return_obj = []        
            for entry in islice(obj, iter_item_list[i], iter_item_list[i+1]):
                json_obj = json.dumps(entry, default=self.json_cleaner)
                return_obj.append(json_obj)
    
            yield return_obj


    def publish(self, obj, batch_name, batch_size = 10):
        """Publish Command

        Args:
            obj(list (dict)): a list of dicts to be published tuple by tuple
            batch_name (str): Does nothing in this case
            batch_size (int): number of records to batch up

        """
        #self._loop.run_until_complete(self._publish_group(obj))

        logger.debug('publish start')

        batches = self._tumbling_window_batcher(obj, batch_size=batch_size)

        for batch in batches:
            asyncio.run(self._aio_publish_msg(json.dumps(batch, default=self.json_cleaner)))
        #asyncio.run(self._aio_publish_msg(json.dumps(obj, default=self.json_cleaner)))
        
        #for dictionary in obj:
        #    result = json.dumps(dictionary, default=self.json_cleaner)            
        #    self.producer.produce(self.topic, result.encode('utf-8'), callback=self._delivery_report)
        #    self.producer.poll(0.1)
        
        logger.debug('publish call stop')

        #self.producer.flush()

    #async def _publish_group(self, msg_bunch):
    #    coros = [self._aio_publish_msg(json.dumps(msgdict, default=self.json_cleaner)) for msgdict in msg_bunch ]
    #    await asyncio.gather(*coros)

    async def _aio_publish_msg(self, msgbody):
        """asyncio publisher
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)

        # .encode('utf-8')
        self.producer.produce(self.topic, msgbody, on_delivery=ack)
        
        return result