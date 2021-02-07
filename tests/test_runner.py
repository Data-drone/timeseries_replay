# Test functions for the main runner
import pytest
from timeseries_replay.central_runner.runner import CentralRunner
from timeseries_replay.database_connector.db_connector import DataBaseConnector
from timeseries_replay.publishers.console_publisher import DebugPublisher
import datetime
import time
import logging
import os

os.makedirs('test_tmp', exist_ok=True)


@pytest.mark.parametrize("replay_rate", [0.1, 1, 2, 3])
def test_batch_generator(replay_rate):

    # test basic functionality of the central runner batch generator
    runner = CentralRunner(db_connection='mock_connection', 
                            output_system='mock_output_systerm', 
                            start_time=datetime.datetime(2020, 5, 17, 13, 0, 0), 
                            end_time=datetime.datetime(2020, 5, 17, 13, 0, 59),
                            replay_rate=replay_rate )

    for batch in runner._batch_generator():
        start = batch[0]
        end = batch[1]

        assert isinstance(start, datetime.datetime)
        assert isinstance(end, datetime.datetime)
        assert end > start

        if end != runner.replay_end_time:
            assert batch[1]-batch[0] == datetime.timedelta(seconds=runner.replay_rate) 

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
    date_diff = datetime.datetime.now() - time_start
    start = time.perf_counter()
    
    runner._trigger_release(result_set=results_test, date_diff=date_diff, replay_start_time=time_start, 
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
    caplog.set_level(logging.INFO)

    session = dataset

    start_date = datetime.datetime(2020, 5, 17, 13, 0, 0)
    end_date = datetime.datetime(2020, 5, 17, 13, 0, 5)
    replay_rate = 1 

    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column='timestamp', 
                                    start_date=start_date,
                                    end_date=end_date)

    runner = CentralRunner(db_connection=db_connector_test, 
                            output_system='mock_output_systerm', 
                            start_time=start_date, 
                            end_time=end_date,
                            replay_rate=replay_rate )

    start = time.perf_counter()
    
    runner.run()

    end = time.perf_counter()

    code_time = end - start
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

    debug_publisher = DebugPublisher()

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
