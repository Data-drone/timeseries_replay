import logging
from sqlalchemy import create_engine, inspect, Table
from sqlalchemy.schema import MetaData
# database connector class to return a session for querying
from dateutil.parser import *

logger = logging.getLogger(__name__)

class DataBaseConnector:

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

        Checks:
            table exists
            table column exists
            table column covers the requested date period
            start_date < end_date
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
        assert(parse(parsed_obj[0]['min_date'])<=self.start_date)
        assert(parse(parsed_obj[0]['max_date'])>=self.start_date)
        assert(parse(parsed_obj[0]['max_date'])>=self.end_date)

        assert self.start_date < self.end_date

    def query_data(self, start, end):

        grab_batch = 'select * from {0} where {1} >= "{2}" AND {1} < "{3}"'.format(self.table_name, self.time_column, start, end)
        results = self.session.execute(grab_batch)
        parsed_obj = [{key: value for key, value in row.items()} for row in results if row is not None]
        
        return parsed_obj
        

