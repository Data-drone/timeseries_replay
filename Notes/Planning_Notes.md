# Planning

Planning notes for what we will need to get this usable

- TODO
    - need to write some examples on usage
    - install script
    - I don't think we need the start_date / end_date as part of reader initialisation?

- Later TODO
    - get more publisher tests written

## MVP - Alpha

1)  [x] Read from SQLAlchemy DB with timestamp column
Inputs:
    - [x] connection string
    - [x] tablename
    - [x] timestamp column

2) [x] Starting a job:
    - [x] start timestamp
    - [x] end timestamp
    - [x] replay rate 
    - [x] batch_size - using replay rate as the batch size for now

2) [x] Publishers for:
    - [x] Console 
    - [x] Kafka
    - [x] File output

### Structure:

Connection Class -> read in initial settings / contain the connection object  

A job class -> split out the querying so that we are stuck if it db layer is running slow

Output Class -> Needs to connect to output system in certain cases / provide some sort of write functionality

Central Runner:
- Creates job list based on the replay rate / batch size
- Sends the job to extract the dataset
- Sends to the output system -> check offset diff -> and if <0 send 

### Central Runner

Setting up the intervals

start time / end time / replay rate / current time
timeinterval = end time - start time
The length of each batch is the replay rate
For 1 second we will have second by second batches
What will we do if a batch is empty? skip?
We are hitting timing issues with a writer that is writing json due to IO at the moment - Fixed with AsyncIO 

#### Central Runner Logic rethink

Timeline

start_date ------------------- end_date

datetime_now ................. new_end

## Database Class

We want to make it flexible and work with different databases
Take db flavour specific code and leave that to sqlalchemy
But sqlalchemy is focused around Object models as the backing db for apps

Do we want to move the creation of the engine into the main execution loop rather than via the database class? - Yes done

Tested with SQLite / Postgres

## Writer Class

Current writers:

    - Console
    - File
    - Kafka
        - Kafka has an overhead with starting a connection to pushing a batch to getting data back.
        - Need to make the `_triger_release` non-blocking
        - Need to add in workers for the publisher logic so that a slow `publish` won't result in delay a subsequent publish at the next timestamp 
### Things to consider

Returning Data back in a timely manner
working out and timing the query to run next to get the next back of data
How will we read and batch up the tasks?
How are we maintaining the central clock and cadence

