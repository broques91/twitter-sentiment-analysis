FROM bitnami/spark:3.2.1
USER root

COPY requirements.txt ./requirements.txt
RUN pip install -r ./requirements.txt
RUN pip install pyspark==3.2.1

USER 1001