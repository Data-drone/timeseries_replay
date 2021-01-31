import pytest
from timeseries_replay.database_connector.db_connector import DataBaseConnector


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Float, String, DateTime, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

from datetime import datetime

Base = declarative_base()

class Timeseries(Base):
    """Testing class to create our dataset"""

    __tablename__ = 'timeseries_dataset'

    timestamp = Column(DateTime(timezone=False), nullable=False)
    textstring = Column(String, nullable=True)
    numbervalue = Column(Float, nullable=True)
    # Adding just cause sqlalchemy needs it
    # See: https://medium.com/@geoffreykoh/fun-with-fixtures-for-database-applications-8253eaf1a6d
    pk_col = Column(Integer, primary_key=True, autoincrement=True)


# TODO read up more on what scope does
@pytest.fixture(scope='function')
def setup_database():
    """Setup our test database fixture"""

    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

@pytest.fixture(scope='function')
def dataset(setup_database):
    """Load Dummy Data """

    session = setup_database

    #Create entries
    t1 = Timeseries(timestamp=datetime(2020, 5, 17, 13, 0, 0), textstring='test1', numbervalue=45.1)
    t2 = Timeseries(timestamp=datetime(2020, 5, 17, 13, 0, 1), textstring='test2', numbervalue=12.3)
    t3 = Timeseries(timestamp=datetime(2020, 5, 17, 13, 0, 3), textstring='test3', numbervalue=0.11)
    t4 = Timeseries(timestamp=datetime(2020, 5, 17, 13, 0, 5), textstring='test4', numbervalue=-32)
    t5 = Timeseries(timestamp=datetime(2020, 5, 17, 13, 0, 5), textstring='test5', numbervalue=232)

    session.add(t1)
    session.add(t2)
    session.add(t3)
    session.add(t4)
    session.add(t5)
    session.commit()

    yield session

@pytest.mark.parametrize("time_column", ['timestamp'])
def test_database_session(dataset, time_column):

    session = dataset

    db_connector_test = DataBaseConnector(session=session, 
                                    table_name='timeseries_dataset', 
                                    time_column=time_column, 
                                    start_date=datetime(2020, 5, 17, 13, 0, 0),
                                    end_date=datetime(2020, 5, 17, 13, 0, 5))

    db_connector_test.startup_checks()

