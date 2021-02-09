from timeseries_replay.database_connector.file_connector import ParquetFileConnector
import datetime

def test_parquet_file_connector():

    path = 'test_data/test_data.parquet'
    time_column = 'requesttimestamp'
    start_date = datetime.datetime(2020, 7, 10, 0, 1, 0)
    end_date = datetime.datetime(2020, 7, 10, 0, 2, 0)

    fileconnector = ParquetFileConnector(path=path, time_column=time_column, 
                                    start_date=start_date, end_date=end_date)

    fileconnector.startup_checks()

    query_start = datetime.datetime(2020, 7, 10, 0, 1, 0)
    query_end = datetime.datetime(2020, 7, 10, 0, 1, 1)

    data_snapshot = fileconnector.query_data(start=query_start, end=query_end)

    assert len(data_snapshot) > 0
    assert data_snapshot[0]['requesttimestamp'] is not None
    
