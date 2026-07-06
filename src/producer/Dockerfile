FROM python:3.12.1-slim

WORKDIR /app

ADD ./producer/producer.py /app/
ADD ./producer/publish.py /app/
ADD config.py /app/
ADD events.avsc /app/

USER root
RUN pip install urllib3 requests confluent-kafka confluent-kafka[avro] \
    pydantic pydantic-settings tenacity

ENTRYPOINT [ "python3", "/app/publish.py" ]