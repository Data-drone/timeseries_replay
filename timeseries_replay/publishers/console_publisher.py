import json
import logging
import os
from timeseries_replay.publishers.BasePublisher import BasePublisher

logger = logging.getLogger(__name__)

class ConsolePublisher(BasePublisher):

    def __init__(self):
        
        logging.info('Initiating Console Publisher')
        
    def publish(self, obj, batch_name):
        for dictionary in obj:
            result = json.dumps(dictionary)
            print(result)

class DebugPublisher(BasePublisher):
    """
    Publisher class for debugging outputs and making sure that it is returning the right data

    Dumps it to file as we do not return data from the run method
    So we will need to check from the file dumps

    """

    def __init__(self):
        
        logging.info('Initiating Debug Publisher')
        
    def publish(self, obj, batch_name):
        """
        Notes:

        TODO writing the files seem to cause issues with latency

        """

        for int, dictionary in enumerate(obj):

            assert dictionary != {}

            folder = os.path.join('test_tmp', batch_name) 
            name = os.path.join(folder, str(int)+'.json')
            os.makedirs(folder, exist_ok=True)
            with open(name, 'w') as fp:
                json.dump(dictionary, fp)
