# Test functions for the main runner
import pytest
from timeseries_replay.central_runner.runner import CentralRunner
import datetime

@pytest.mark.parametrize("replay_rate", [0.1, 1, 2, 3])
def test_batch_generator(replay_rate):

    # test basic functionality of the central runner batch generator
    runner = CentralRunner(db_connection='mock_connection', 
                            output_system='mock_output_systerm', 
                            start_time=datetime.datetime(2020, 5, 17, 13, 0, 0), 
                            end_time=datetime.datetime(2020, 5, 17, 13, 0, 59),
                            replay_rate=replay_rate )

    for batch in runner.batch_generator():
        start = batch[0]
        end = batch[1]

        assert isinstance(start, datetime.datetime)
        assert isinstance(end, datetime.datetime)
        assert end > start

        if end != runner.replay_end_time:
            assert batch[1]-batch[0] == datetime.timedelta(seconds=runner.replay_rate) 