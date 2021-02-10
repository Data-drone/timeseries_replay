# Test functions for the main runner
import pytest
from timeseries_replay.central_runner.runner import CentralRunner
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

# WIP
@pytest.mark.parametrize("replay_rate", [0.5, 1, 2, 3, 4])
def test_trigger_release(replay_rate):
    # test that we are triggering releasing of datasets to output properly
    replay_rate = replay_rate
    start_time = datetime.datetime(2020, 5, 17, 13, 0, 0)
    end_time = datetime.datetime(2020, 5, 17, 13, 0, 59)

    assert end_time > start_time
    assert (end_time - start_time).total_seconds() > 0
    period_duration = (end_time - datetime.timedelta(seconds=replay_rate) - start_time).total_seconds()

    # test basic functionality of the central runner batch generator
    runner = CentralRunner(db_connection='mock_connection', 
                            output_system='mock_output_systerm', 
                            start_time=start_time, 
                            end_time=end_time,
                            replay_rate=replay_rate )

    code_start_time = datetime.datetime.now()

    test_tuple = [{'mock_result'}]

    code_start = time.perf_counter()
    
    for batch in runner._batch_generator():
        dataset = runner._trigger_release(result_set=test_tuple, 
                                            code_start=code_start_time, 
                                        replay_start_time=runner.replay_start_time, 
                                        batch=batch, 
                                        replay_rate=runner.replay_rate)
    
    code_end = time.perf_counter()

    actual_length = code_end - code_start
    target_length = period_duration/replay_rate
    assert abs(actual_length - target_length) < 1
