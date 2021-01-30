# Planning

Planning notes for what we will need to get this usable

## MVP


1)
Read from SQLAlchemy DB with timestamp column
Inputs:
- connection string
- tablename
- timestamp column

Considerations
- datatypes? Do we need to do anything or just string json and push?

Starting a job:
- start timestamp
- end timestamp
- replay rate
- batch_size

2)
Publisher to console log for now?
Target is Kafka later on
Also need to be able to push to files too


Structure:

Connection Class -> read in initial settings / contain the connection object  

A job class -> split out the querying so that we are stuck if it db layer is running slow

Output Class -> Needs to connect to output system in certain cases / provide some sort of write functionality

Central Runner:
- Creates job list based on the replay rate / batch size
- Sends the job to extract the dataset
- Sends to the output system -> check offset diff -> and if <0 send 

### Things to consider

Returning Data back in a timely manner
working out and timing the query to run next to get the next back of data

How will we read and batch up the tasks?

How are we maintaining the central clock and cadence
