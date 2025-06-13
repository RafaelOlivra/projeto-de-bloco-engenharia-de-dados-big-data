from kafka import KafkaConsumer
import json
import os
from datetime import datetime
import boto3

# Kafka config
KAFKA_TOPIC = 'weather'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']

# MinIO config
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY = os.environ['MINIO_ROOT_PASSWORD']
BUCKET_NAME = 'raw'

# Inicializa o consumidor Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Conecta ao MinIO
s3 = boto3.client('s3',
    endpoint_url=f'http://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

# Verifica se o bucket existe
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
except:
    s3.create_bucket(Bucket=BUCKET_NAME)

# Consume and upload
for message in consumer:
    data = message.value
    city = data['city']
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    key = f"weather/{city}/{timestamp}.json"
    
    print(f"📥 Salvando dados de clima no MinIO como {key}")
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data).encode('utf-8'))
