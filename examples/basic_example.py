"""Basic Example

Read from file and spit to console

"""


from timeseries_replay.database_connector.file_connector import ParquetFileConnector
from timeseries_replay.central_runner.runner import CentralRunner
from timeseries_replay.publishers.console_publisher import ConsolePublisher
import datetime
import time


replay_start = datetime.datetime(2020, 7, 10, 0, 1, 0)
replay_end = datetime.datetime(2020, 7, 10, 0, 5, 0)
replay_rate = 1 # 1 = real time

reader = ParquetFileConnector(path='test_data/test_data.parquet',
                            time_column='requesttimestamp',
                            start_date=replay_start,
                            end_date=replay_end)

reader.startup_checks()

publisher = ConsolePublisher()

runner = CentralRunner(db_connection=reader,
                        output_system=publisher,
                        start_time=replay_start,
                        end_time=replay_end,
                        replay_rate=replay_rate)

start=time.perf_counter()
runner.run()
end=time.perf_counter()

minutes_taken = (end-start)/60
actual_time = replay_end - replay_start

time_taken = print('Time Taken to run = {0}, Actual Time = {1}'.format(minutes_taken, actual_time))