import pytest

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, DateTime, Float, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime

# postgres fixtures
from pytest_postgresql import factories


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

@pytest.fixture(scope='function')
def big_dataset(setup_database):
    """Load Dummy Data """

    session = setup_database

    #Create entries
    t1 = [{'timestamp':datetime(2020, 5, 17, 13, 0, 0), 'textstring':'test1', 'numbervalue':45.1},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 1), 'textstring':'test2', 'numbervalue':12.3},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test31', 'numbervalue':0.11},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test32', 'numbervalue':0.12},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test33', 'numbervalue':0.14},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test34', 'numbervalue':0.11},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test35', 'numbervalue':0.1},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test36', 'numbervalue':12.11},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test37', 'numbervalue':3.11},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test38', 'numbervalue':1.11},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 3), 'textstring':'test39', 'numbervalue':4.11},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 5), 'textstring':'test4', 'numbervalue':-32},
        {'timestamp':datetime(2020, 5, 17, 13, 0, 5), 'textstring':'test5', 'numbervalue':232}]

    session.bulk_insert_mappings(Timeseries, t1)
    session.commit()

    yield session


postgresql = factories.postgresql_noproc(host='db', port=5432, user='postgres', password='t3st1ng')

@pytest.fixture(scope='function')
def postgres_fixture(postgresql):
    
    connection = 'postgresql+psycopg2://postgres:t3st1ng@db:5432/postgres'
    engine = create_engine(connection, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

@pytest.fixture(scope='function')
def psql_dataset(postgres_fixture):
    """Load Dummy Data """

    session = postgres_fixture

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