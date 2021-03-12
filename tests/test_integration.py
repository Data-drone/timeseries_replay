# Test functions for the main runner
import pytest
from timeseries_replay.central_runner.runner import CentralRunner
from timeseries_replay.database_connector.db_connector import DataBaseConnector
from timeseries_replay.publishers.console_publisher import FilePublisher, ConsolePublisher
from timeseries_replay.database_connector.file_connector import ParquetFileConnector
from timeseries_replay.publishers.kafka_publisher import KafkaPublisher
import datetime
import time
import logging
import os

os.makedirs('test_tmp', exist_ok=True)

test_pairs = [(datetime.datetime(2021, 1, 1, 10, 0, 59),1),
                (datetime.datetime(2021, 1, 1, 10, 0, 58),2),
                (datetime.datetime(2021, 1, 1, 10, 0, 57),3),]

@pytest.mark.parametrize("time_start,time_diff", test_pairs)
def test_runner_with_db(dataset, time_start, time_diff):
    """Adding in db_connector

    add in a DB Connector as the next step 
    so that we can test out the functionality together


    """

    session = dataset

    end_date = datetime.datetime(2020, 5, 17, 13, 0, 5)
    replay_rate = 1.0 

    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column='timestamp', 
                                    start_date=time_start,
                                    end_date=end_date)

    runner = CentralRunner(db_connection=db_connector_test, 
                            output_system='mock_output_systerm', 
                            start_time=time_start, 
                            end_time=end_date,
                            replay_rate=replay_rate )


    results_test = [
        {'timestamp': datetime.datetime(2021, 1, 1, 10, 1, 0), 'text': 'bob', 'value': 10.0},
        {'timestamp': datetime.datetime(2021, 1, 1, 10, 1, 1), 'text': 'cat', 'value':-10.0},
        {'timestamp': datetime.datetime(2021, 1, 1, 10, 1, 1), 'text': 'eat', 'value': 12.1}
    ]
    
    # test that the trigger_release is working right
    # expect 1
    start = time.perf_counter()
    
    code_start = datetime.datetime.now()

    # we need to retink the way that we trigger this....
    runner._trigger_release(result_set=results_test, code_start=code_start, replay_start_time=time_start, 
    batch=(datetime.datetime(2021, 1, 1, 10, 1, 0), datetime.datetime(2021, 1, 1, 10, 1, 1)), 
    replay_rate=replay_rate)
    
    end = time.perf_counter()

    code_time = end - start
    assert int(code_time) == time_diff


def test_runner_full_loop(caplog, dataset):
    """test the full loop

    test dataset goes from datetime(2020, 5, 17, 13, 0, 0)
    to datetime(2020, 5, 17, 13, 0, 5)
    4 seconds total

    """
    caplog.set_level(logging.DEBUG)

    session = dataset

    start_date = datetime.datetime(2020, 5, 17, 13, 0, 0)
    end_date = datetime.datetime(2020, 5, 17, 13, 0, 5)
    replay_rate = 1 
    
    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column='timestamp', 
                                    start_date=start_date,
                                    end_date=end_date)

    test_publisher = ConsolePublisher()

    runner = CentralRunner(db_connection=db_connector_test, 
                            output_system=test_publisher, 
                            start_time=start_date, 
                            end_time=end_date,
                            replay_rate=replay_rate )

    start = time.perf_counter()
    
    runner.run()

    end = time.perf_counter()

    code_time = end - start
    
    print(code_time)
    
    assert int(code_time) == 4


def test_runner_full_loop_big(caplog, big_dataset):
    """test the a bigger loop

    test a bigger fixture and check return tuples

    """
    caplog.set_level(logging.INFO)

    session = big_dataset

    start_date = datetime.datetime(2020, 5, 17, 13, 0, 3)
    end_date = datetime.datetime(2020, 5, 17, 13, 0, 6)
    replay_rate = 1 

    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column='timestamp', 
                                    start_date=start_date,
                                    end_date=end_date)

    debug_publisher = FilePublisher()

    runner = CentralRunner(db_connection=db_connector_test, 
                            output_system=debug_publisher, 
                            start_time=start_date, 
                            end_time=end_date,
                            replay_rate=replay_rate )

    start = time.perf_counter()
    
    runner.run()

    end = time.perf_counter()

    code_time = end - start
    assert int(code_time) == 2

    ## Test results
    assert os.path.exists('test_tmp/17-05-2020_13-00-03')
    assert os.path.exists('test_tmp/17-05-2020_13-00-05')

    assert len(os.listdir('test_tmp/17-05-2020_13-00-03')) == 9
    assert len(os.listdir('test_tmp/17-05-2020_13-00-05')) == 2

#import shutil
#shutil.rmtree('test_tmp/*')

@pytest.mark.parametrize("replay_rate", [1, 2])
def test_runner_large_file(caplog, replay_rate):
    """
    Test system under load from the large test parquet

    """

    caplog.set_level(logging.INFO)

    path = 'test_data/test_data.parquet'
    time_column = 'requesttimestamp'
    start_date = datetime.datetime(2020, 7, 10, 0, 1, 0)
    end_date = datetime.datetime(2020, 7, 10, 0, 3, 0)
    replay_rate = replay_rate
    bootstrap_servers = 'kafka:9092'
    topic = 'test_stream_2'

    logging.debug("starting")

    fileconnector = ParquetFileConnector(path=path, time_column=time_column, 
                                    start_date=start_date, end_date=end_date)

    fileconnector.startup_checks()

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )

    runner = CentralRunner(db_connection=fileconnector, 
                            output_system=publisher, 
                            start_time=start_date, 
                            end_time=end_date,
                            replay_rate=replay_rate )

    start = time.perf_counter()
    
    runner.run()

    #publisher.close()

    end = time.perf_counter()

    code_time = end - start

    period_duration = (end_date - datetime.timedelta(seconds=replay_rate) - start_date).total_seconds()

    assert abs(code_time - period_duration/replay_rate) < 1
    #assert int(code_time) == (end_date - start_date).total_seconds() * replay_rate


# 4x and 8x may still be too much for what the code can currently handle
@pytest.mark.parametrize("replay_rate", [0.5, 1])
def test_runner_long_duration(caplog, replay_rate):
    """
    Test system under load from the large test parquet

    """

    caplog.set_level(logging.INFO)

    path = 'test_data/test_data.parquet'
    time_column = 'requesttimestamp'
    start_date = datetime.datetime(2020, 7, 10, 0, 1, 0)
    end_date = datetime.datetime(2020, 7, 10, 0, 5, 0)
    replay_rate = replay_rate
    bootstrap_servers = 'kafka:9092'
    topic = 'test_stream_2'

    fileconnector = ParquetFileConnector(path=path, time_column=time_column, 
                                    start_date=start_date, end_date=end_date)

    fileconnector.startup_checks()

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic
    )

    runner = CentralRunner(db_connection=fileconnector, 
                            output_system=publisher, 
                            start_time=start_date, 
                            end_time=end_date,
                            replay_rate=replay_rate )

    start = time.perf_counter()
    
    runner.run()

    #publisher.close()

    end = time.perf_counter()

    code_time = end - start

    period_duration = (end_date - datetime.timedelta(seconds=replay_rate) - start_date).total_seconds()

    assert abs(code_time - period_duration/replay_rate) < 1

    #assert int(code_time) == (end_date - start_date).total_seconds() * replay_rate
