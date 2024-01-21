#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors
import json
from uuid import uuid4
import requests

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "raw"
CITIES = ["Belgrade", "Novi Sad", "Subotica", "Kragujevac", "Niš"]

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

def get_uuid():
    return str(uuid4())

def fetch_weather_data(city):
    api_url = f'http://api.weatherapi.com/v1/current.json?key=5ea4340e491244e490f221127241601&q={city}&aqi=no'

    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data for {city}. Status code: {response.status_code}")
        return None

while True:
    for city in CITIES:
        weather_data = fetch_weather_data(city)

        if weather_data:
            print(f'sending weather data to kafka topic {TOPIC} - {weather_data}')
            producer.send(TOPIC, key=bytes(get_uuid(), 'utf-8'), value=json.dumps(weather_data).encode('utf-8'))
        
        time.sleep(180)  # Wait for 3 minutes before fetching data for the next city
