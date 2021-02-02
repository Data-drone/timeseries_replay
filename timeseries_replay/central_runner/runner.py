import logging
import datetime
from datetime import timedelta
from dateutil.parser import *
import time
import math
# this is the main loop that triggers the data reads and data output
#
#

logger = logging.getLogger(__name__)

class CentralRunner:
    # this class creates and manages the scheduling of the replay system

    def __init__(self, db_connection, output_system, 
                    start_time, end_time, replay_rate):

        """Construct the central runner object
        This calculates the batches and triggers the db reads 
           and writing to the output system
        
        Args:
            db_connection: a db_connector object
            output_system: an output system object
            start_time (datetime): start datetime for the replay
            end_time (datetime): end datetime for the replay
            replay_rate (float): the rate at which we should replay the data 1 is realtime 
        """

        
        self.db_system = db_connection
        self.output_system = output_system
        self.replay_start_time = start_time
        self.replay_end_time = end_time
        self.replay_rate = replay_rate
        
    def run(self):
        """Main function that triggers the core logic

        loops through fetches data then send off to the output system

        """ 

        date_diff = datetime.datetime.now() - self.replay_start_time

        for batch in self._batch_generator():

            result_set = self.db_system.query_data(batch[0], batch[1])
            dataset = self._trigger_release(result_set, date_diff, self.replay_start_time, batch, self.replay_rate)
            # release dataset to writer here


    def _trigger_release(self, result_set, date_diff, replay_start_time, batch, replay_rate):
        """Function to trigger the release of an event to the output system
        
        Args:
            result_set (dict): the tuples that will be sent off into the output system
            date_diff (datetime): The difference between the current time and the start of the dataset
                                    we need to rebase the timestamps to release at the right intervals
            batch (tuple(datetime, datetime)): tuple of dates to get in the  

        """
        
        offset = (batch[0] - replay_start_time).total_seconds() * replay_rate # 
        current_offset = datetime.datetime.now() - replay_start_time ## This is wrong
        batch_offset = timedelta(seconds=offset) + date_diff
        
        wait_time = (batch_offset - current_offset).total_seconds()
        logger.info('batch_starts at: {0} we ar waiting for {1} secs'.format(batch[0], wait_time))

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
            
            batch_end = min(batch_start + timedelta(seconds=self.replay_rate), 
                            self.replay_end_time)
            
            logger.debug("Yielding Batch {0}, {1}".format(batch_start, batch_end))
            
            yield (batch_start, batch_end)
            batch_start = batch_end
