"""Database Connector Class

The database connector class takes a valid SQLAlchemy session then executes queries and checks that parameters provided are logical 
"""

import logging
from sqlalchemy import create_engine, inspect, Table
from sqlalchemy.schema import MetaData
# database connector class to return a session for querying
from dateutil.parser import *

logger = logging.getLogger(__name__)

class DataBaseConnector:
    """Database Connector Class

    Receives a valid sqlalchemy session then checks the input parameters and queries for data

    Attributes:
        session (sqlalchemy.orm.session.Session): valid SQLAlchemy session for the class to query
        table_name (str): name as a string of the table some databases may require <schema.table_name>
        time_column (str): the name of the column that will be used to see when tuples were logged
        start_date (datetime): start datetime for when to start the streaming
        end_date (datetime): end datetime for when to stop the streaming

    """

    def __init__(self, session, table_name, time_column, start_date, end_date):
        """DB Connector Object
        
        """

        self.session = session
        self.table_name = table_name
        self.time_column = time_column
        self.start_date = start_date
        self.end_date = end_date

    def startup_checks(self):
        """Startup checks to make sure that we have the right inputs

        This function cchecks:
            that the table exists
            that the table column exists
            that the table column covers the requested date period
            that the start_date < end_date
        """

        # check timeseries table and column definition
        table_check = 'select count({0}) as row_count from {1}'.format(self.time_column, self.table_name)
        table_result_object = self.session.execute(table_check)
        parsed_col = [{key: value for key, value in row.items()} for row in table_result_object if row is not None]
        assert(parsed_col[0]['row_count']>0)

        min_max_dates = 'select min({0}) as min_date, \
                        max({0}) as max_date from {1}'.format(self.time_column, self.table_name)
        min_date_result_object = self.session.execute(min_max_dates).fetchall()
        assert min_date_result_object is not None
        parsed_obj = [{key: value for key, value in row.items()} for row in min_date_result_object if row is not None]
        
        min_date_parse = parsed_obj[0]['min_date']
        if type(min_date_parse) == str:
            min_date_parse = parse(min_date_parse)

        max_date_parse = parsed_obj[0]['max_date']
        if type(max_date_parse) == str:
            max_date_parse = parse(max_date_parse)

        assert(min_date_parse<=self.start_date)
        assert(max_date_parse>=self.start_date)
        assert(max_date_parse>=self.end_date)

        assert self.start_date < self.end_date

    def query_data(self, start, end):
        """Querys a batch

        Args:
            start (datetime): start of batch
            end (datetime): end of batch

        """

        grab_batch = 'select * from {0} where {1} >= "{2}" AND {1} < "{3}"'.format(self.table_name, self.time_column, start, end)
        results = self.session.execute(grab_batch)
        parsed_obj = [{key: value for key, value in row.items()} for row in results if row is not None]
        
        return parsed_obj
        

