# Test functions for the main runner
from timeseries_replay.central_runner.runner import CentralRunner
import datetime

def test_batch_generator():

    # test basic functionality of the central runner batch generator

    runner = CentralRunner(db_connection='mock_connection', 
                            output_system='mock_output_systerm', 
                            start_time=datetime.datetime(2020, 5, 17, 13, 0, 0), 
                            end_time=datetime.datetime(2020, 5, 17, 14, 0, 59),
                            replay_rate=1 )

    for batch in runner.batch_generator():
        start = batch[0]
        end = batch[1]

        assert isinstance(start, datetime.datetime)
        assert isinstance(end, datetime.datetime)
    #    assert batch[1] >= batch[0]