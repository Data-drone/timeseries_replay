"""A template Publisher class of what methods we need for the runner
"""
import logging
import datetime

logger = logging.getLogger(__name__)

class BasePublisher:

    """Base Publisher class
    """

    def __init__(self):
        pass

    def json_cleaner(self, item):
        """Clean json formats

        Json dumps doesn't know how to deal with datetime

        Args:
            item (object): Anytime json dumps doesn't know how to parse something it will
                            go here. As we see more exotic datatypes may need to expand this

        Returns:
            item (str): String version for the outputting to json

        """

        if isinstance(item, datetime.datetime):
            return item.__str__()

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
