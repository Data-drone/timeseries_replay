import json
import logging

logger = logging.getLogger(__name__)

class ConsolePublisher:

    def __init__(self):
        
        logging.info('Initiating Console Publisher')
        
    def publish(self, obj):
        for dictionary in obj:
            result = json.dumps(dictionary)
            print(result)