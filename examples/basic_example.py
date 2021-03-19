"""Basic Example

Read from file and spit to console

"""


from timeseries_replay.database_connector.file_connector import ParquetFileConnector
from timeseries_replay.central_runner.runner import CentralRunner
from timeseries_replay.publishers.console_publisher import ConsolePublisher
import datetime

reader = ParquetFileConnector(path='test_data/test_data.parquet',
                            time_column='requesttimestamp',
                            start_date=datetime.datetime(2020, 7, 10, 0, 1, 0),
                            end_date=datetime.datetime(2020, 7, 10, 0, 3, 0))

reader.startup_checks()

publisher = ConsolePublisher()

runner = CentralRunner(db_connection=reader,
                        output_system=publisher,
                        start_time=datetime.datetime(2020, 7, 10, 0, 1, 0),
                        end_time=datetime.datetime(2020, 7, 10, 0, 3, 0),
                        replay_rate=1)

runner.run()