FROM python:3.9


WORKDIR /produce
COPY . .

RUN pip install -r requirements.txt

# EXPOSE 8000
# HEALTHCHECK CMD xxx

CMD ["python3", "producer.py"]