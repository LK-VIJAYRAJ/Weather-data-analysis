import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

class WeatherAnalytics:
    def __init__(self, weather_model):
        self.weather_model = weather_model
    
    def temperature_trend_analysis(self, city, days=30):
        """Analyze temperature trends over time"""
        start_date = datetime.utcnow() - timedelta(days=days)
        end_date = datetime.utcnow()
        
        data = self.weather_model.get_weather_by_date_range(
            start_date, end_date, city
        )
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Calculate moving averages
        df['temp_ma_7'] = df['temperature'].rolling(window=7).mean()
        df['temp_ma_14'] = df['temperature'].rolling(window=14).mean()
        
        # Calculate trend
        x = np.arange(len(df))
        trend_coeff = np.polyfit(x, df['temperature'], 1)[0]
        
        return {
            'city': city,
            'period_days': days,
            'data_points': len(df),
            'avg_temperature': df['temperature'].mean(),
            'min_temperature': df['temperature'].min(),
            'max_temperature': df['temperature'].max(),
            'temperature_trend': 'increasing' if trend_coeff > 0 else 'decreasing',
            'trend_slope': trend_coeff,
            'daily_data': df[['timestamp', 'temperature', 'temp_ma_7', 'temp_ma_14']].to_dict('records')
        }
    
    def weather_pattern_analysis(self, city, days=90):
        """Analyze weather patterns and frequencies"""
        start_date = datetime.utcnow() - timedelta(days=days)
        end_date = datetime.utcnow()
        
        data = self.weather_model.get_weather_by_date_range(
            start_date, end_date, city
        )
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        
        # Weather condition frequency
        weather_conditions = df['weather'].apply(lambda x: x.get('main', 'Unknown'))
        condition_counts = weather_conditions.value_counts()
        
        # Humidity analysis
        humidity_stats = df['humidity'].describe()
        
        # Wind analysis
        wind_speeds = df['wind'].apply(lambda x: x.get('speed', 0))
        wind_stats = wind_speeds.describe()
        
        return {
            'city': city,
            'analysis_period': days,
            'total_records': len(df),
            'weather_patterns': condition_counts.to_dict(),
            'humidity_analysis': humidity_stats.to_dict(),
            'wind_analysis': wind_stats.to_dict(),
            'temperature_variance': df['temperature'].var()
        }
    
    def extreme_weather_detection(self, city, threshold_percentile=95):
        """Detect extreme weather events"""
        # Get last 6 months of data for baseline
        start_date = datetime.utcnow() - timedelta(days=180)
        end_date = datetime.utcnow()
        
        data = self.weather_model.get_weather_by_date_range(
            start_date, end_date, city
        )
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        
        # Calculate thresholds
        temp_high_threshold = np.percentile(df['temperature'], threshold_percentile)
        temp_low_threshold = np.percentile(df['temperature'], 100 - threshold_percentile)
        humidity_high_threshold = np.percentile(df['humidity'], threshold_percentile)
        wind_high_threshold = np.percentile(
            df['wind'].apply(lambda x: x.get('speed', 0)), 
            threshold_percentile
        )
        
        # Identify extreme events
        extreme_events = []
        
        for record in data:
            temp = record['temperature']
            humidity = record['humidity']
            wind_speed = record['wind'].get('speed', 0)
            timestamp = record['timestamp']
            
            if (temp >= temp_high_threshold or temp <= temp_low_threshold or
                humidity >= humidity_high_threshold or wind_speed >= wind_high_threshold):
                
                extreme_events.append({
                    'timestamp': timestamp,
                    'temperature': temp,
                    'humidity': humidity,
                    'wind_speed': wind_speed,
                    'weather_condition': record['weather'].get('main', ''),
                    'severity_score': self._calculate_severity_score(
                        temp, humidity, wind_speed,
                        temp_high_threshold, temp_low_threshold,
                        humidity_high_threshold, wind_high_threshold
                    )
                })
        
        return {
            'city': city,
            'extreme_events': extreme_events,
            'thresholds': {
                'temperature_high': temp_high_threshold,
                'temperature_low': temp_low_threshold,
                'humidity_high': humidity_high_threshold,
                'wind_speed_high': wind_high_threshold
            }
        }
    
    def _calculate_severity_score(self, temp, humidity, wind_speed, 
                                temp_high, temp_low, humidity_high, wind_high):
        """Calculate severity score for extreme weather events"""
        score = 0
        
        if temp >= temp_high:
            score += (temp - temp_high) / temp_high
        elif temp <= temp_low:
            score += (temp_low - temp) / temp_low
        
        if humidity >= humidity_high:
            score += (humidity - humidity_high) / humidity_high
        
        if wind_speed >= wind_high:
            score += (wind_speed - wind_high) / wind_high
        
        return min(score, 10.0)  # Cap at 10
    
    def generate_weather_forecast_accuracy(self, city, days=30):
        """Analyze forecast accuracy (if historical forecasts are stored)"""
        # This would require storing both forecast and actual data
        # Implementation depends on your forecasting data structure
        pass
