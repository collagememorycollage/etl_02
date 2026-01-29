FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

COPY create_db.sql create_db.sql

ENTRYPOINT ["python", "data.py"]
