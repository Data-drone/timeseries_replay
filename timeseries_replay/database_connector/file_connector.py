""" File Connector to read from files
Sometimes we may want to just read from a flat file rather than firing up a db
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

class ParquetFileConnector:
    """Parquet File Connector Class

    reads from a fixed parquet file and streams it out 

    Attributes:
        path (str): File path to a parquet file
        time_column (str): The specific time column that we need to reference
        start_date (datetime): Start Date for the timeseries streaming 
        end_date (datetime): End Date for the timeseries streaming
    """


    def __init__(self, path, time_column, start_date, end_date):
        """Initialise object
        """

        logger.info('starting parquet file loader')

        self.time_column = time_column
        self.start_date = start_date
        self.end_date = end_date

        self.df = pd.read_parquet(path=path)

        # could cause issue in some settings?
        try:
            self.df = self.df.set_index([self.time_column])
        except KeyError:
            logger.error('Missing column: {0} Available are: {1}'.format(time_column, self.df.columns))

    def startup_checks(self):
        """Startup checks to make sure that we have the right inputs

        This function cchecks:
            that the table exists
            that the table column exists
            that the table column covers the requested date period
            that the start_date < end_date
        """

        assert(len(self.df)>0)

        min_date = self.df.index.min().to_pydatetime()
        max_date = self.df.index.max().to_pydatetime()

        assert min_date <= self.start_date
        assert max_date >= self.end_date
        
        assert self.start_date < self.end_date


    def query_data(self, start, end):
        """Querys a batch

        For pandas datetime index matching need to have it in string format

        Args:
            start (datetime): start of batch
            end (datetime): end of batch

        Returns:
            results (list(dict)): Returns a list of dics from our pandas frame for streaming out

        """
        start_string =  start.strftime("%Y-%m-%d %H:%M:%S")
        end_string = end.strftime("%Y-%m-%d %H:%M:%S")

        results = self.df.loc[start_string:end_string]
        results[self.time_column] = results.index
        return results.to_dict('records')
