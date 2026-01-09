import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    DATABASE_NAME = os.getenv('DATABASE_NAME', 'weather_analytics')
    WEATHER_API_KEY = os.getenv('WEATHER_API_KEY', 'your_api_key_here')
    WEATHER_API_URL = 'http://api.openweathermap.org/data/2.5'
    BATCH_SIZE = 1000
    DATA_RETENTION_DAYS = 365
