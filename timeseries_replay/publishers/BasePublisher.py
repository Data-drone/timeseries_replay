"""A template Publisher class of what methods we need for the runner
"""
import logging

logger = logging.getLogger(__name__)

class BasePublisher:

    """Base Publisher class
    """

    def __init__(self):
        pass

    def publish(self, obj, batch_name):
        """Publish Method

        Args:
            obj (list(dict)): the objects that need to be published
            batch_name (str): used by some publishers to help output the datasets
        """

        pass

    def close(self):
        """Close Method

        Depending on the output publisher we don't necessarily need to do anything here
        For some publishers we may need to close threads and join async routines

        """
        logger.info('close action')
