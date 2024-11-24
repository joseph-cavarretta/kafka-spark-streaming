FROM bitnami/spark:3

WORKDIR /app

ADD ./consumer/consumer.py /app/
ADD ./consumer/spark_streaming.py /app/
ADD config.json /app/
ADD events.avsc /app/

USER root
RUN pip install requests fastavro confluent-kafka pyspark

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0", "/app/spark_streaming.py"]