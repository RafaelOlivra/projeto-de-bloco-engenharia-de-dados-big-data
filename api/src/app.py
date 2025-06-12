import os
import pandas as pd
import s3fs
import numpy as np
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer
from lib.Utils import Utils

from models.AirQuality import AirQualityRecord
from models.AirQualityAnomaly import AirQualityAnomalyRecord
from models.Weather import WeatherRecord
from models.WeatherAnomaly import WeatherAnomalyRecord

# MinIO/S3 configs
minio_endpoint = os.environ['FASTAPI_MINIO_ENDPOINT']
minio_user = os.environ['FASTAPI_MINIO_USER']
minio_password = os.environ['FASTAPI_MINIO_PASSWORD']

s3_fs = s3fs.S3FileSystem(
    key=minio_user,
    secret=minio_password,
    client_kwargs={'endpoint_url': minio_endpoint}
)

# FastAPI app
app = FastAPI()

########### Dados ###########

def weather_data(parquet_path: str = "refined/weather_with_coords") -> pd.DataFrame:
    """Busca os dados meteorológicos no MinIO/S3 e retorna como um DataFrame."""
    try:
        df = pd.read_parquet(parquet_path, filesystem=s3_fs)

        # Ordena por data
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp').reset_index(drop=True)

        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler o arquivo parquet: {str(e)}")
    

def weather_anomaly_data(parquet_path: str = "refined/weather_anomalies_with_coords") -> pd.DataFrame:
    """Busca os dados de anomalias meteorológicas no MinIO/S3 e retorna como um DataFrame."""
    try:
        df = pd.read_parquet(parquet_path, filesystem=s3_fs)

        # Ordena por data
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp').reset_index(drop=True)

        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler o arquivo parquet: {str(e)}")
    

def airquality_data(parquet_path: str = "refined/airquality_with_coords") -> pd.DataFrame:
    """Busca os dados de qualidade do ar no MinIO/S3 e retorna como um DataFrame."""
    try:
        df = pd.read_parquet(parquet_path, filesystem=s3_fs)

        # Ordena por data
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp').reset_index(drop=True)

        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler o arquivo parquet: {str(e)}")

def airquality_anomaly_data(parquet_path: str = "refined/airquality_anomalies_with_coords") -> pd.DataFrame:
    """Busca os dados de anomalias na qualidade do ar no MinIO/S3 e retorna como um DataFrame."""
    try:
        df = pd.read_parquet(parquet_path, filesystem=s3_fs)

        # Ordena por data
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp').reset_index(drop=True)

        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler o arquivo parquet: {str(e)}")

########### Rotas ###########

@app.get("/")
def welcome():
    return {"message": "Bem-vindo à API TP3 Weather!"}

@app.get("/weather", response_model=list[WeatherRecord], tags=["weather"])
def get_weather_data(
    limit: int = 1000,
    offset: int = 0,
    date_from: str = "",
    date_to: str = "",
    uf: str = "",
    city: str = "",
):
    """
    Recupera dados meteorológicos com filtragem opcional por data.

    Argumentos:
        limit (int): Número máximo de registros a serem retornados.
        offset (int): Número de registros a serem ignorados (pular).
        date_from (str): Data inicial para filtragem (formato ISO).
        date_to (str): Data final para filtragem (formato ISO).
        uf (str): Unidade Federativa para filtragem (opcional).
        city (str): Cidade para filtragem (opcional).

    Retorna:
        list[WeatherRecord]: Lista de registros meteorológicos.
    """
    try:
        df = weather_data()

        # Filtro por data, se fornecido
        if date_from:
            df = df[df['timestamp'] >= Utils.to_datetime(date_from)]
        if date_to:
            df = df[df['timestamp'] <= Utils.to_datetime(date_to)]

        # Filtro por UF e cidade, se fornecido
        if uf:
            df = df[df['uf'].str.upper() == uf.upper()]
        if city:
            df = df[df['city'].str.upper() == city.upper()]

        return df.to_dict(orient='records')[offset:offset + limit]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados meteorológicos: {str(e)}")


@app.get("/weather/anomalies", response_model=list[WeatherAnomalyRecord], tags=["weather"])
def get_weather_anomaly_data(
    limit: int = 1000,
    offset: int = 0,
    date_from: str = "",
    date_to: str = "",
    uf: str = "",
    city: str = "",
):
    """
    Recupera dados de anomalias meteorológicas com filtragem opcional por data.

    Argumentos:
        limit (int): Número máximo de registros a serem retornados.
        offset (int): Número de registros a serem ignorados (pular).
        date_from (str): Data inicial para filtragem (formato ISO).
        date_to (str): Data final para filtragem (formato ISO).
        uf (str): Unidade Federativa para filtragem (opcional).
        city (str): Cidade para filtragem (opcional).

    Retorna:
        list[WeatherAnomalyRecord]: Lista de registros de anomalias meteorológicas.
    """
    try:
        df = weather_anomaly_data()

        # Filtro por data, se fornecido
        if date_from:
            df = df[df['timestamp'] >= Utils.to_datetime(date_from)]
        if date_to:
            df = df[df['timestamp'] <= Utils.to_datetime(date_to)]

        # Filtro por UF e cidade, se fornecido
        if uf:
            df = df[df['uf'].str.upper() == uf.upper()]
        if city:
            df = df[df['city'].str.upper() == city.upper()]

        return df.to_dict(orient='records')[offset:offset + limit]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados de anomalias meteorológicas: {str(e)}")

@app.get("/weather/anomalies/recent", response_model=list[WeatherAnomalyRecord], tags=["weather"])
def get_recent_weather_anomaly_data(
    limit: int = 1000,
    offset: int = 0,
    uf: str = "",
    city: str = "",
):
    """
    Recupera dados de anomalias meteorológicas recentes (últimas 2 horas) com filtragem opcional por UF e cidade.

    Argumentos:
        limit (int): Número máximo de registros a serem retornados.
        offset (int): Número de registros a serem ignorados (pular).
        uf (str): Unidade Federativa para filtragem (opcional).
        city (str): Cidade para filtragem (opcional).

    Retorna:
        list[WeatherAnomalyRecord]: Lista de registros de anomalias meteorológicas.
    """
    try:
        df = weather_anomaly_data(parquet_path="refined/weather_recent_anomalies_with_coords")

        # Filtro por UF e cidade, se fornecido
        if uf:
            df = df[df['uf'].str.upper() == uf.upper()]
        if city:
            df = df[df['city'].str.upper() == city.upper()]

        return df.to_dict(orient='records')[offset:offset + limit]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados recentes de anomalias meteorológicas: {str(e)}")


@app.get("/airquality", response_model=list[AirQualityRecord], tags=["airquality"])
def get_airquality_data(
    limit: int = 1000,
    offset: int = 0,
    date_from: str = "",
    date_to: str = "",
    uf: str = "",
    city: str = "",
):
    """
    Recupera dados da qualidade do ar com filtragem opcional por data.

    Argumentos:
        limit (int): Número máximo de registros a serem retornados.
        offset (int): Número de registros a serem ignorados (pular).
        date_from (str): Data inicial para filtragem (formato ISO).
        date_to (str): Data final para filtragem (formato ISO).
        uf (str): Unidade Federativa para filtragem (opcional).
        city (str): Cidade para filtragem (opcional).

    Retorna:
        list[AirQualityRecord]: Lista de registros da qualidade do ar.
    """
    try:
        df = airquality_data()

        # Filtro por data, se fornecido
        if date_from:
            df = df[df['timestamp'] >= Utils.to_datetime(date_from)]
        if date_to:
            df = df[df['timestamp'] <= Utils.to_datetime(date_to)]

        # Filtro por UF e cidade, se fornecido
        if uf:
            df = df[df['uf'].str.upper() == uf.upper()]
        if city:
            df = df[df['city'].str.upper() == city.upper()]

        return df.to_dict(orient='records')[offset:offset + limit]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados da qualidade do ar: {str(e)}")


@app.get("/airquality/anomalies", response_model=list[AirQualityAnomalyRecord], tags=["airquality"])
def get_airquality_anomaly_data(
    limit: int = 1000,
    offset: int = 0,
    date_from: str = "",
    date_to: str = "",
    uf: str = "",
    city: str = "",
):
    """
    Recupera dados de anomalias na qualidade do ar com filtragem opcional por data.

    Argumentos:
        limit (int): Número máximo de registros a serem retornados.
        offset (int): Número de registros a serem ignorados (pular).
        date_from (str): Data inicial para filtragem (formato ISO).
        date_to (str): Data final para filtragem (formato ISO).
        uf (str): Unidade Federativa para filtragem (opcional).
        city (str): Cidade para filtragem (opcional).

    Retorna:
        list[AirQualityAnomalyRecord]: Lista de registros de anomalias na qualidade do ar.
    """
    try:
        df = airquality_anomaly_data()
        df = df.replace([np.nan, float('inf'), float('-inf')], None)
        df['std_value'] = df['std_value'].replace([np.nan, float('inf'), float('-inf')], 0)
        df['aqi_z_score'] = df['std_value'].replace([np.nan, float('inf'), float('-inf')], 0)

        # Filtro por data, se fornecido
        if date_from:
            df = df[df['timestamp'] >= Utils.to_datetime(date_from)]
        if date_to:
            df = df[df['timestamp'] <= Utils.to_datetime(date_to)]

        # Filtro por UF e cidade, se fornecido
        if uf:
            df = df[df['uf'].str.upper() == uf.upper()]
        if city:
            df = df[df['city'].str.upper() == city.upper()]

        return df.to_dict(orient='records')[offset:offset + limit]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados de anomalias na qualidade do ar: {str(e)}")
    
@app.get("/airquality/anomalies/recent", response_model=list[AirQualityAnomalyRecord], tags=["airquality"])
def get_recent_airquality_anomaly_data(
    limit: int = 1000,
    offset: int = 0,
    uf: str = "",
    city: str = "",
):
    """
    Recupera dados de anomalias na qualidade do ar recentes (últimas 2 horas) com filtragem opcional por UF e cidade.

    Argumentos:
        limit (int): Número máximo de registros a serem retornados.
        offset (int): Número de registros a serem ignorados (pular).
        uf (str): Unidade Federativa para filtragem (opcional).
        city (str): Cidade para filtragem (opcional).

    Retorna:
        list[AirQualityAnomalyRecord]: Lista de registros de anomalias na qualidade do ar.
    """
    try:
        df = airquality_anomaly_data(parquet_path="refined/airquality_recent_anomalies_with_coords")
        df = df.replace([np.nan, float('inf'), float('-inf')], None)
        df['std_value'] = df['std_value'].replace([np.nan, float('inf'), float('-inf')], 0)
        df['aqi_z_score'] = df['std_value'].replace([np.nan, float('inf'), float('-inf')], 0)

        # Filtro por UF e cidade, se fornecido
        if uf:
            df = df[df['uf'].str.upper() == uf.upper()]
        if city:
            df = df[df['city'].str.upper() == city.upper()]

        return df.to_dict(orient='records')[offset:offset + limit]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar dados recentes de anomalias na qualidade do ar: {str(e)}")
