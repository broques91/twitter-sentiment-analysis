FROM python:3.9


WORKDIR /produce
COPY . .

RUN pip install -r requirements.txt

CMD ["python3", "producer.py"]