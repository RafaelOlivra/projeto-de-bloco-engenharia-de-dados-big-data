import json, time, requests, os, sys
from kafka import KafkaProducer

# Permite incluir o diretório pai no sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

from utils.cities import parse_ranges, filter_cities

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ranges = parse_ranges(os.getenv("WEATHER_RANGES", ""))
cities = filter_cities(ranges)

def get_weather(city, lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()["current_weather"]
            data["city"] = city
            return data
    except Exception as e:
        print(f"Erro ao buscar o clima para {city}: {e}")
    return None

while True:
    for city, coords in cities.items():
        weather = get_weather(city, coords["lat"], coords["lon"])
        if weather:
            print(f"Enviando dados de clima de {city}: {weather}")
            producer.send("weather", weather)

    # Atraso aleatório entre 5 e 15 segundos
    random_delay = 5 + (time.time() % 10)

    # Espera 15 minutos mais o atraso aleatório
    time.sleep(15 * 60 + random_delay)
