import json, time, requests, os, sys
from kafka import KafkaProducer

# Include parent directory for shared modules
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
    print(f"Fetching air quality for {city} at coordinates ({lat}, {lon})")
    api_key = os.getenv("WAQI_API_KEY")
    url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") != "ok":
            print(f"WAQI API returned status: {data.get('status')} for {city}")
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
            print(f"Sending air quality for {city}: {record}")
            producer.send("airquality", record)

    except Exception as e:
        print(f"Error fetching air quality for {city}: {e}")

while True:
    for city, coords in cities.items():
        fetch_air_quality(city, coords["lat"], coords["lon"])
    random_delay = 5 + (time.time() % 10)  # Random delay between 5–15s
    time.sleep(15 * 60 + random_delay)
