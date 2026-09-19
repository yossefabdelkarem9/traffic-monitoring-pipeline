FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY --chown=airflow:root ./wheels /tmp/wheels

RUN pip install --no-cache-dir \
      /tmp/wheels/pyspark-3.5.1.tar.gz \
      /tmp/wheels/kafka_python-3.0.11-py3-none-any.whl \
      /tmp/wheels/psycopg2_binary-2.9.13-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.whl \
      /tmp/wheels/py4j-0.10.9.7-py2.py3-none-any.whl \
  && rm -rf /tmp/wheels