FROM apache/airflow:2.9.1
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

USER root
RUN apt-get update \
    && apt-get install -yqq --no-install-recommends \ 
        apt-utils \
        pkg-config \
        iputils-ping \
        curl \
        rsync \
        git \
        subversion 
