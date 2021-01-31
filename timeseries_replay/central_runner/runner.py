import logging
from datetime import timedelta

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

        """

        pass

    def batch_generator(self):
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
