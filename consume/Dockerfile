FROM python:3.8


WORKDIR /consume
COPY . .

RUN pip install -r requirements.txt

CMD ["python3", "consumer.py"]