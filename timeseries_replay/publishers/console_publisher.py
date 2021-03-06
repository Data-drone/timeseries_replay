"""Basic Data Publishers

The Data Publishers take the queried data from the db_connectors then send it out into an output format

"""

import json
import logging
import os
import aiofiles
import datetime
import asyncio
from timeseries_replay.publishers.BasePublisher import BasePublisher

logger = logging.getLogger(__name__)

class ConsolePublisher(BasePublisher):
    """Console Publisher

    The console publisher publishes received data directly into the console via print

    """

    def __init__(self):
        super().__init__()
        
        logging.info('Initiating Console Publisher')
        
    def publish(self, obj, batch_name):
        """publish data

        Args:
            obj(list(dict)): a list of dict objects to publish tuple by tuple
            batch_name(str): not used in this particular publisher

        """
        for dictionary in obj:
            result = json.dumps(dictionary, default=self.json_cleaner)
            print(result)

class FilePublisher(BasePublisher):
    """File Publisher
    Publisher class for debugging outputs and making sure that it is returning the right data

    Dumps it to file as we do not return data from the run method
    So we will need to check from the file dumps.
    Can also be used to test systems that harvest and watch folders for new files appearing

    """

    def __init__(self, output_folder='test_tmp'):
        super().__init__()

        self.output_folder = output_folder
        
        logging.info('Initiating Debug Publisher')
        
    def publish(self, obj, batch_name):
        """Publish Data

        Publishes data into a file
        each batch is written into a separate folder
        writes data using asyncio to ensure that we achieve the throughput required
        
        Args:
            obj(list(dict)): A list of dictionaries to publish tuple by tuple
            batch_name(str): used to create subfolders for each tuple to be written in
        """

        for int, dictionary in enumerate(obj):

            assert dictionary != {}

            folder = os.path.join(self.output_folder, batch_name) 
            name = os.path.join(folder, str(int)+'.json')
            os.makedirs(folder, exist_ok=True)

            asyncio.run(self._write_data(dictionary, name))

    async def _write_data(self, dump_object, name):
        """Async writer

        async process to write out files 

        """

        async with aiofiles.open(name, 'w') as fp:
                data = json.dumps(dump_object, indent = 1, default=self.json_cleaner)
                await fp.write(data)
