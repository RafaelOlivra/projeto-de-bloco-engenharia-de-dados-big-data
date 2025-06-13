FROM python:3.12.9-slim

RUN apt-get update && \
    apt-get install -y build-essential \
    && apt-get install openjdk-17-jre curl -y\
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN rm /work/requirements.txt

ENV JARS_PATH=/usr/local/lib/python3.12/site-packages/pyspark/jars
COPY jars/* $JARS_PATH/

EXPOSE 8888 4040

ENV JUPYTER_TOKEN=changeme

CMD ["jupyter", "lab", "--port-retries=0", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=${JUPYTER_TOKEN}", "--ServerApp.password="]