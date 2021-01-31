import logging
from sqlalchemy import create_engine, inspect, Table
from sqlalchemy.schema import MetaData
# database connector class to return a session for querying
#

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
        """Check the provided database session

        Since we intend to just connect to an arbritary database and table
        we should check before initiating the dataloading
        rather than check logic only in tests

        """

        # check timeseries column
        table_check = 'select {0} from {1}'.format(self.time_column, self.table_name)
        result_object = self.session.execute(table_check)
        
        
        


class DBConnector:

    def __init__(self, connection_string, table_name, schema, time_column, start_date, end_date):
        
        """Construct the database connection
        Setup the connection a database and make sure that has the information that we need

        Args:
            connection_string (string): valid sqlalchemy connection string
            table_name (string): name of the table that we will be replaying
            schema (string): name of the db schema the table is in
            time_column (string): name of the column holding the timedata for replay
            start_date (): 

        """

        self.table_schema = schema
        self.timeseries_table = table_name
        self.time_column = time_column
        self.start_date = start_date
        self.end_date = end_date

        self.engine = create_engine(connection_string)
        self.db_metadata = MetaData()
        #self.connection = self.engine.connect()

    def startup_checks(self):

        insp = inspect(self.engine) 
        if insp.has_table(self.timeseries_table, schema=self.schema) is False:
            raise ValueError("table: (0) doesn't exist".format(self.timeseries_table))



        #timeseries_table = Table(self.timeseries_table, self.db_metadata, autoload=True, autoload_with=self.engine)
        #columns = [c.name for c in timeseries_table.columns]

        #if self.time_column not in columns:
        #    raise ValueError("column: {0} doesn't exist in table: {1}".format(self.time_column, self.timeseries_table))