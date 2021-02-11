"""Example Script

"""

import datetime

db_connection_string = 'postgresql+psycopg2://<username>:<password>@<database_host>:<db_port>/<database>'
timeseries_table_to_stream = '<your table>'
timecolumn = '<your timestamp column name>'
start_date = datetime.datetime(2020, 5, 17, 13, 0, 5)
end_date = datetime.datetime(2020, 5, 17, 13, 1, 5)
replay_rate = 1.0 # Real time
kafka_bootstrap_servers = '<kafka_broker>:<kafka_port>'
output_topic = '<kafka topic to output tuples to>'