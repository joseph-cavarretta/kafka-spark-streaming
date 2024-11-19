FROM python:3.12.1-slim

ADD producer.py /
ADD publish.py /

RUN pip3 install confluent-kafka

ENTRYPOINT [ "python3", "./publish.py" ]