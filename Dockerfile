FROM python:3.11.6-slim

RUN apt-get update && apt-get install -y git

RUN apt-get update && apt-get install -y chromium-driver

# Install Python dependencies.
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /app
COPY ./app /app
WORKDIR /app

# RUN mkdir /app/run
# COPY ./run /app/run

CMD ["/bin/sh", "/app/run.sh"]