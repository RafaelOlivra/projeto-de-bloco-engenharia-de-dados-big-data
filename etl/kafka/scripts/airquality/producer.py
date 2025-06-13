import json, time, requests, os, sys
from kafka import KafkaProducer

# Inclui o diretório pai para módulos compartilhados
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

from utils.cities import parse_ranges, filter_cities

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ranges = parse_ranges(os.getenv("WEATHER_RANGES", ""))
cities = filter_cities(ranges)

def fetch_air_quality(city, lat, lon):
    print(f"Buscando qualidade do ar para {city} nas coordenadas ({lat}, {lon})")
    api_key = os.getenv("WAQI_API_KEY")
    url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") != "ok":
            print(f"A API WAQI retornou o status: {data.get('status')} para {city}")
            return

        aqi_data = data.get("data", {})
        iaqi = aqi_data.get("iaqi", {})
        timestamp = aqi_data.get("time", {}).get("s")

        for pollutant, value_data in iaqi.items():
            record = {
                "city": city,
                "parameter": pollutant,
                "value": value_data.get("v"),
                "unit": "AQI",
                "timestamp": timestamp,
                "source": "WAQI"
            }
            print(f"Enviando dados de qualidade do ar de {city}: {record}")
            producer.send("airquality", record)

    except Exception as e:
        print(f"Erro ao buscar a qualidade do ar para {city}: {e}")

while True:
    for city, coords in cities.items():
        fetch_air_quality(city, coords["lat"], coords["lon"])
    
    # Atraso aleatório entre 5 e 15 segundos
    random_delay = 5 + (time.time() % 10)
    
    # Espera 15 minutos mais um pequeno atraso aleatório
    time.sleep(15 * 60 + random_delay)
