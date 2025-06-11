FROM apache/airflow:2.10.4-python3.12

USER root

# Atualiza o apt com suporte a HTTPS e adiciona repositórios base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    apt-transport-https \
    curl \
    gnupg && \
    echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:$PATH"

RUN source ~/.bashrc

ENV JARS_PATH=/usr/local/lib/python3.12/site-packages/pyspark/jars
COPY jars/* $JARS_PATH/

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN rm requirements.txt

RUN chown -R 50000:0 /opt/airflow/logs
RUN chmod -R 775 /opt/airflow/logs