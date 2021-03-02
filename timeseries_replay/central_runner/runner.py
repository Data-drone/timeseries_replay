
"""Main Runner Module that coordinates everything

The runner is the main scheduler and thread orchestrater for undertaking the replay


"""

import logging
import datetime
from dateutil.parser import *
import time
import math
import asyncio

logger = logging.getLogger(__name__)

class CentralRunner:
    """Central Runner Class

    Coordinates the running of the application to read data from a source system then stream it to an output device

    Attributes:
        db_connection (sqlalchemy.orm.session.Session): a db_connector object
        output_system (BasePublisher): an output system object
        start_time (datetime.datetime): start datetime.datetime for the replay
        end_time (datetime.datetime): end datetime.datetime for the replay
        replay_rate (float): the rate at which we should replay the data 1 is realtime 

    Returns:
        CentralRunner object for use in triggering jobs and running them

    """

    def __init__(self, db_connection, output_system, 
                    start_time, end_time, replay_rate):
        """Inits the class with the specified parameters"""

        self.db_system = db_connection
        self.output_system = output_system
        self.replay_start_time = start_time
        self.replay_end_time = end_time
        self.replay_rate = replay_rate
        self.batch_size = replay_rate
        
    def run(self):
        """Main function that triggers the core logic

        Generates batches based on the replay rate and the start / end date that we wish to replay
        Depending on the start time of the batch, the main loop may pause to wait before pushing the batch to be published
        Batches get passed to the Database session for querying
        Query Results get passed to the output system to publish

        TODO - We are using asyncio here so what about non async publishers? - to Check

        """ 

        code_start = datetime.datetime.now()

        for batch in self._batch_generator():

            start_query = time.perf_counter()
            result_set = self.db_system.query_data(batch[0], batch[1])
            end_query = time.perf_counter()

            query_time = end_query - start_query
            logger.info("query took {0}".format(query_time))
            
            dataset = self._trigger_release(result_set, code_start, self.replay_start_time, 
                                                batch, self.replay_rate)
            # release dataset to writer here
            # we might need to adjust this to keep running with none?
            if dataset is not None and type(self.output_system) != str:

                start_output = time.perf_counter()
                self.output_system.publish(dataset, batch[0].strftime("%d-%m-%Y_%H-%M-%S"))
                #asyncio.run(self.output_system.publish(dataset, batch[0].strftime("%d-%m-%Y_%H-%M-%S")))
                end_output = time.perf_counter()

                output_timer = end_output - start_output
                logger.info("output took {0}".format(output_timer))
        
        
        self.output_system.close()
            
    def _trigger_release(self, result_set, code_start, replay_start_time, batch, replay_rate):
        """Function to trigger the release of an event to the output system

        Args:
            result_set (dict): the tuples that will be sent off into the output system
            code_start (datetime.datetime): the start time of the code itself
            replay_start_time (datetime.datetime):  
            batch (tuple(datetime.datetime, datetime.datetime)): tuple of dates in the batch
            replay_rate (float): rate at which to replay the data back

        """
        
        # need to divide by the replay to make sure 2 to double time
        # seconds offset from the replay start 
        batch_offset = (batch[0] - replay_start_time).total_seconds() / replay_rate  

        # current offset is what the replay time at the time in this trigger operation
        # does current offset need to adjust to replay rate? no it doesn't
        # there isn't a total_seconds?
        current_offset = (datetime.datetime.now() - code_start).total_seconds() # / replay_rate        
        logger.info('batch_offset is: {0} current_offset is: {1}'.format(batch_offset, current_offset))

        wait_time = (batch_offset - current_offset)

        logger.info('batch_starts at: {0} we are waiting for {1} secs'.format(batch[0], wait_time))

        if wait_time > 0:
            time.sleep(wait_time)

        return result_set

    def _batch_generator(self):
        """Setup batches to send to db for querying

        loops through and spits out the start and end times for us to fetch the data from the database 

        """

        logger.info("Running Batch Generator")

        batch_start = self.replay_start_time
        while batch_start < self.replay_end_time:
            
            batch_end = min(batch_start + datetime.timedelta(seconds=self.batch_size), 
                            self.replay_end_time)
            
            logger.debug("Yielding Batch {0}, {1}".format(batch_start, batch_end))
            
            yield (batch_start, batch_end)
            batch_start = batch_end
