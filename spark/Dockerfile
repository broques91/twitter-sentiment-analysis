FROM bitnami/spark:3
USER root

COPY requirements-dev.txt ./requirements.txt
RUN pip install -r ./requirements.txt
USER 1001