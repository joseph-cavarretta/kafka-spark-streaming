FROM bitnami/spark:3

ADD consumer.py /
ADD spark_streaming.py /
ADD config.json /
ADD events.avsc /

RUN pip3 install confluent-kafka pyspark

ENTRYPOINT [ "spark-submit", "./spark_streaming.py" ]