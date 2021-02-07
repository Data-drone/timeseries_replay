FROM python:3.8-slim
COPY . /timeseries_replay
WORKDIR /timeseries_replay
RUN pip install --no-cache-dir -r requirements.txt
RUN ["pytest", "-v"]
CMD tail -f /dev/null