import requests
import logging
from datetime import datetime
from config import Config

class WeatherDataIngester:
    def __init__(self, weather_model):
        self.weather_model = weather_model
        self.api_key = Config.WEATHER_API_KEY
        self.api_url = Config.WEATHER_API_URL
    
    def fetch_current_weather(self, city):
        """Fetch current weather data from API"""
        try:
            url = f"{self.api_url}/weather"
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            return self.format_weather_data(data)
        
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for {city}: {e}")
            return None
    
    def fetch_historical_weather(self, city, days=7):
        """Fetch historical weather data"""
        weather_records = []
        
        for day in range(days):
            timestamp = datetime.utcnow().timestamp() - (day * 86400)
            url = f"{self.api_url}/onecall/timemachine"
            params = {
                'lat': 0,  # You'll need to geocode the city first
                'lon': 0,
                'dt': int(timestamp),
                'appid': self.api_key,
                'units': 'metric'
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if 'current' in data:
                    formatted_data = self.format_weather_data(data['current'], city)
                    weather_records.append(formatted_data)
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"Historical data fetch failed for {city}: {e}")
        
        return weather_records
    
    def format_weather_data(self, raw_data, city_override=None):
        """Format API data for MongoDB storage"""
        return {
            'timestamp': datetime.fromtimestamp(raw_data.get('dt', 0)),
            'location': {
                'city': city_override or raw_data.get('name', 'Unknown'),
                'country': raw_data.get('sys', {}).get('country', ''),
                'coordinates': {
                    'lat': raw_data.get('coord', {}).get('lat', 0),
                    'lon': raw_data.get('coord', {}).get('lon', 0)
                }
            },
            'temperature': raw_data.get('main', {}).get('temp', 0),
            'feels_like': raw_data.get('main', {}).get('feels_like', 0),
            'humidity': raw_data.get('main', {}).get('humidity', 0),
            'pressure': raw_data.get('main', {}).get('pressure', 0),
            'weather': {
                'main': raw_data.get('weather', [{}])[0].get('main', ''),
                'description': raw_data.get('weather', [{}])[0].get('description', ''),
                'icon': raw_data.get('weather', [{}])[0].get('icon', '')
            },
            'wind': {
                'speed': raw_data.get('wind', {}).get('speed', 0),
                'direction': raw_data.get('wind', {}).get('deg', 0)
            },
            'visibility': raw_data.get('visibility', 0),
            'uv_index': raw_data.get('uvi', 0)
        }
    
    def ingest_multiple_cities(self, cities):
        """Ingest data for multiple cities"""
        weather_records = []
        
        for city in cities:
            weather_data = self.fetch_current_weather(city)
            if weather_data:
                weather_records.append(weather_data)
        
        if weather_records:
            return self.weather_model.bulk_insert_weather_data(weather_records)
        
        return None
