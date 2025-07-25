FROM prefecthq/prefect:3.2.1-python3.12

RUN apt-get update && apt-get install -y git chromium-driver

COPY requirements.txt /opt/prefect/docker_reporting/requirements.txt
RUN python -m pip install -r /opt/prefect/docker_reporting/requirements.txt

COPY . /opt/prefect/docker_reporting/
WORKDIR /opt/prefect/docker_reporting/