# Timeseries Replay

Streaming Data frequently crosses different systems, frameworks and applications.
This makes it really hard to architect for and test.

Generating fake data to load test often fails to replicate real life scenarios.
Sometimes it maybe useful to take an existing events table with timestamps and replay those at the same rate (or perhaps sped up rate)

this application will hopefully be able to do some of that

## Current Sources

Read: 
- SQLAlchemy compatible sources

Write:
- Folder on Disk
- Console
- Kafka

## Notes

- Currently the Kafka publisher isn't async and can bottleneck here depending on the number of messages being published

## Running Tests

- TODO add how to trigger test in the right docker container from host commandline

```{bash}

# start containers
docker-compose -f docker_compose/kafka_testing.yml up


# still need to test this
# docker exec -it docker_compose_replay_service_1 /timeseries_replay/pytest
```

### Cheat notes
Running individual tests

```{bash}

# run specific test file
pytest tests/<file>.py

# specific function
pytest tests/test_integration.py::test_runner_large_file

# specific function with logging to console
pytest tests/test_integration.py::test_runner_large_file -o log_cli=True

```

