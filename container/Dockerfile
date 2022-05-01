FROM gcr.io/spark-operator/spark-py:v3.1.1

USER root:root

RUN mkdir -p /app

COPY ../conf /app/conf

COPY ../experimental /app/experimental

COPY ../kafka_io /app/kafka_io

COPY ../utils /app/utils

COPY ../requirements.txt /app
COPY ../jars /opt/spark/jars/

WORKDIR /app

RUN pip install -r requirements.txt

