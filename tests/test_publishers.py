from timeseries_replay.publishers.console_publisher import FilePublisher
import datetime
import os

def test_console_publisher():

    publisher = FilePublisher(output_folder='tmp')

    test = [{'timestamp': '2020-01-01 10:00:00', 'test1': 'A', 'testB': 0 },
            {'timestamp': '2020-01-01 10:00:01', 'test1': 'B', 'testB': 1 },
            {'timestamp': '2020-01-01 10:00:02', 'test1': 'C', 'testB': 2 },
            {'timestamp': '2020-01-01 10:00:03', 'test1': 'D', 'testB': 3 },
            ]

    publisher.publish(test, 'test')

    file_list = os.listdir('tmp/test')
    
    assert len(file_list) == 4
