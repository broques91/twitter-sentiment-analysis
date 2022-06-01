FROM python:3.8-alpine3.11

RUN apk update && apk add build-base

WORKDIR /produce
COPY . .

RUN pip install -r requirements.txt

# EXPOSE 8000
# HEALTHCHECK CMD xxx

CMD ["python3", "producer.py"]