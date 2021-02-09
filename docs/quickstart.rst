Quickstart
==========

Getting Started
---------------

The SQL Alchemy session and Output system connections need to be created prior to leveraging this module.
See SQLAlchemy docs for more details::

    # Input Parameters required
    db_connection_string = 'postgresql+psycopg2://<username>:<password>@<database_host>:<db_port>/<database>'
    timeseries_table_to_stream = '<your table>'
    timecolumn = '<your timestamp column name>'
    start_date = datetime.datetime(2020, 5, 17, 13, 0, 5)
    end_date = datetime.datetime(2020, 5, 17, 13, 1, 5)
    replay_rate = 1.0 # Real time
    kafka_bootstrap_servers = '<kafka_broker>:<kafka_port>'
    output_topic = '<kafka topic to output tuples to>'

    # Starting a session
    engine = create_engine(db_connection_string, echo=False)
    SessionObject = sessionmaker(bind=engine)
    session = SessionObject()

    # Passing session to db_connector
    db_connector = DataBaseConnector(session=session,
                                table_name=timeseries_table_to_stream,
                                time_column=timecolumn,
                                start_date=start_date,
                                end_date=end_date)

    # Start the Publisher
    kafka_publisher = KafkaPublisher(bootstrap_servers=kafka_bootstrap_servers
                                        topic=output_topic)

    
    # Build the central runner
    runner = CentralRunner(db_connection=db_connector, 
                            output_system=kafka_publisher, 
                            start_time=start_date, 
                            end_time=end_date,
                            replay_rate=replay_rate)

    # run the stream
    runner.run()

