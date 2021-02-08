import pytest
from timeseries_replay.database_connector.db_connector import DataBaseConnector
from datetime import datetime

@pytest.mark.parametrize("time_column", ['timestamp'])
def test_database_session(dataset, time_column):

    session = dataset

    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column=time_column, 
                                    start_date=datetime(2020, 5, 17, 13, 0, 0),
                                    end_date=datetime(2020, 5, 17, 13, 0, 5))

    db_connector_test.startup_checks()

@pytest.mark.parametrize("time_column", ['timestamp'])
def test_psql(psql_dataset, time_column):

    session = psql_dataset

    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column=time_column, 
                                    start_date=datetime(2020, 5, 17, 13, 0, 0),
                                    end_date=datetime(2020, 5, 17, 13, 0, 5))

    db_connector_test.startup_checks()
