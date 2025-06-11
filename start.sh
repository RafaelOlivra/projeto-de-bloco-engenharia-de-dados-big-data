#!/bin/bash

# JARs necessários
declare -A jar_urls
jar_urls["aws-java-sdk-bundle-1.11.1026.jar"]="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar"
jar_urls["hadoop-aws-3.3.4.jar"]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

# Destinos
jar_dest_paths=(
    "./etl/airflow/jars"
    "./etl/pyspark/jars"
    "./etl/spark/jars"
)

# Baixa os JARs caso não existam
for jar in "${!jar_urls[@]}"; do
    url="${jar_urls[$jar]}"
    for dest in "${jar_dest_paths[@]}"; do
        mkdir -p "$dest"
        if [ ! -f "$dest/$jar" ]; then
            echo "Baixando $jar para $dest"
            wget -q "$url" -P "$dest"
        else
            echo "$jar já existe em $dest"
        fi
    done
done

# Permissões do Airflow
sudo chown -R 50000:0 ./data/airflow/logs/
sudo chmod -R 775 ./data/airflow/logs/

# Inicia o Docker Compose com 2 workers Spark
docker-compose down
docker-compose up --build -d --scale spark-worker=2
