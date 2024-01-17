from confluent_kafka import Producer
import json
from uuid import uuid4
import requests


TOPIC = 'raw'
PRODUCER_CONFIG = {
        "bootstrap.servers": 'kafka:9092',
        'client.id': 'python_producer',
}


def get_uuid():
    return str(uuid4())

def fetch_weather_data():
    api_url = 'http://api.weatherapi.com/v1/current.json?key=5ea4340e491244e490f221127241601&q=Belgrade&aqi=no'

    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data. Status code: {response.status_code}")
        return None

def main():
    weather_data = fetch_weather_data()

    if weather_data:
        p = Producer(PRODUCER_CONFIG)
        event_id = get_uuid()
        weather_data['id'] = event_id

        p.produce(
            topic=TOPIC,
            value=json.dumps(weather_data),
        )
        p.flush()

if __name__ == '__main__':
    main()



