FROM python:3.10-slim-bookworm

RUN apt-get update && apt-get install -y git openjdk-17-jre-headless
RUN pip install --upgrade pip
RUN pip install git+https://github.com/DISSC-yale/fairway.git

ENTRYPOINT ["fairway"]
