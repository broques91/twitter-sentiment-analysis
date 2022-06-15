FROM docker.io/bitnami/spark:3
USER root

COPY ./requirements.txt ./requirements-dev.txt
RUN pip install -r ./requirements-dev.txt
USER 1001