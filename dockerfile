FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install apache-beam
RUN pip install apache-beam[gcp]
RUN pip install google-api-python-client
ADD . /home/beam


RUN pip install apache-airflow[gcp_api]
