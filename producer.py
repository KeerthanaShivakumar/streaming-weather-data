"""
Weather Data Producer - Kafka Producer
Fetches weather data from OpenWeather API and publishes to Kafka topic
"""

import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = 'weather-data'
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', 'demo')  # Get free key from openweathermap.org

# Validate key exists
if not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY == 'your_api_key_here':
    raise ValueError(
        "❌ OPENWEATHER_API_KEY not set!\n"
        "Get your free key at: https://openweathermap.org/api\n"
        "Then add to .env file: OPENWEATHER_API_KEY=your_key"
    )

# Cities to monitor
CITIES = [
    {'name': 'Geneva', 'lat': 46.2044, 'lon': 6.1432},
    {'name': 'Zurich', 'lat': 47.3769, 'lon': 8.5417},
    {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
    {'name': 'Paris', 'lat': 48.8566, 'lon': 2.3522},
    {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
]

class WeatherProducer:
    def __init__(self):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1  # Ensure ordering
        )
        print(f"✅ Kafka Producer initialized - Broker: {KAFKA_BROKER}")
    
    def fetch_weather_data(self, city):
        """Fetch current weather data from OpenWeather API"""
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': city['lat'],
                'lon': city['lon'],
                'appid': OPENWEATHER_API_KEY,
                'units': 'metric'  # Celsius
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Transform to our schema
                weather_record = {
                    'city': city['name'],
                    'latitude': city['lat'],
                    'longitude': city['lon'],
                    'timestamp': datetime.utcnow().isoformat(),
                    'temperature': data['main']['temp'],
                    'feels_like': data['main']['feels_like'],
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'weather_condition': data['weather'][0]['main'],
                    'weather_description': data['weather'][0]['description'],
                    'wind_speed': data['wind']['speed'],
                    'cloudiness': data['clouds']['all'],
                    'visibility': data.get('visibility', 0),
                }
                
                return weather_record
            else:
                print(f"❌ API Error for {city['name']}: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error fetching data for {city['name']}: {e}")
            return None
        except (KeyError, ValueError) as e:
            print(f"❌ Data parsing error for {city['name']}: {e}")
            return None
    
    def publish_to_kafka(self, weather_record):
        """Publish weather record to Kafka topic"""
        try:
            # Use city name as partition key for consistent routing
            key = weather_record['city']
            
            future = self.producer.send(
                KAFKA_TOPIC,
                key=key,
                value=weather_record
            )
            
            # Block until send completes (for demo purposes)
            record_metadata = future.get(timeout=10)
            
            print(f"📤 Published: {weather_record['city']} - "
                  f"Temp: {weather_record['temperature']}°C - "
                  f"Topic: {record_metadata.topic} - "
                  f"Partition: {record_metadata.partition} - "
                  f"Offset: {record_metadata.offset}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to publish to Kafka: {e}")
            return False
    
    def run(self, interval=60):
        """
        Main producer loop - fetch and publish weather data
        
        Args:
            interval: Seconds between data fetches (default: 60)
        """
        print(f"🚀 Starting Weather Data Producer...")
        print(f"📍 Monitoring {len(CITIES)} cities")
        print(f"⏱️  Update interval: {interval} seconds")
        print("-" * 60)
        
        try:
            while True:
                batch_start = time.time()
                success_count = 0
                
                # Fetch weather for all cities
                for city in CITIES:
                    weather_data = self.fetch_weather_data(city)
                    
                    if weather_data:
                        if self.publish_to_kafka(weather_data):
                            success_count += 1
                    
                    # Small delay between API calls to avoid rate limiting
                    time.sleep(1)
                
                batch_duration = time.time() - batch_start
                
                print(f"\n✅ Batch complete: {success_count}/{len(CITIES)} records published "
                      f"in {batch_duration:.2f}s")
                print("-" * 60)
                
                # Wait for next interval (adjusted for batch processing time)
                wait_time = max(0, interval - batch_duration)
                if wait_time > 0:
                    print(f"⏳ Waiting {wait_time:.0f}s until next batch...\n")
                    time.sleep(wait_time)
                    
        except KeyboardInterrupt:
            print("\n⚠️  Shutting down producer...")
        finally:
            self.producer.close()
            print("✅ Producer closed gracefully")

def main():
    """Entry point"""
    # Check for API key
    if OPENWEATHER_API_KEY == 'demo':
        print("⚠️  WARNING: Using demo API key. Get your free key at:")
        print("   https://openweathermap.org/api")
        print("   Then set: export OPENWEATHER_API_KEY='your_key_here'")
        print()
    
    producer = WeatherProducer()
    producer.run(interval=60)  # Fetch every 60 seconds

if __name__ == '__main__':
    main()