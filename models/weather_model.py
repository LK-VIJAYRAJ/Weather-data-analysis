from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
import logging

class WeatherModel:
    def __init__(self, connection_string, database_name):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db.weather_data
        self.create_indexes()
    
    def create_indexes(self):
        """Create necessary indexes for efficient querying"""
        self.collection.create_index([("timestamp", DESCENDING)])
        self.collection.create_index([("location.city", ASCENDING)])
        self.collection.create_index([("location.coordinates", "2dsphere")])
        self.collection.create_index([
            ("location.city", ASCENDING),
            ("timestamp", DESCENDING)
        ])
    
    def insert_weather_data(self, weather_record):
        """Insert single weather record"""
        try:
            weather_record['created_at'] = datetime.utcnow()
            result = self.collection.insert_one(weather_record)
            return result.inserted_id
        except Exception as e:
            logging.error(f"Error inserting weather data: {e}")
            return None
    
    def bulk_insert_weather_data(self, weather_records):
        """Insert multiple weather records efficiently"""
        try:
            for record in weather_records:
                record['created_at'] = datetime.utcnow()
            result = self.collection.insert_many(weather_records)
            return result.inserted_ids
        except Exception as e:
            logging.error(f"Error bulk inserting weather data: {e}")
            return None
    
    def get_weather_by_location(self, city, limit=100):
        """Retrieve weather data for specific location"""
        return list(self.collection.find(
            {"location.city": city}
        ).sort("timestamp", -1).limit(limit))
    
    def get_weather_by_date_range(self, start_date, end_date, city=None):
        """Retrieve weather data within date range"""
        query = {
            "timestamp": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        if city:
            query["location.city"] = city
        
        return list(self.collection.find(query).sort("timestamp", 1))
    
    def get_temperature_aggregation(self, city, days=30):
        """Get temperature statistics for location"""
        start_date = datetime.utcnow() - timedelta(days=days)
        
        pipeline = [
            {
                "$match": {
                    "location.city": city,
                    "timestamp": {"$gte": start_date}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_temp": {"$avg": "$temperature"},
                    "min_temp": {"$min": "$temperature"},
                    "max_temp": {"$max": "$temperature"},
                    "count": {"$sum": 1}
                }
            }
        ]
        
        result = list(self.collection.aggregate(pipeline))
        return result[0] if result else None
