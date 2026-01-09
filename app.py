from flask import Flask, render_template, jsonify, request
from config import Config
from models.weather_model import WeatherModel
from services.data_ingester import WeatherDataIngester
from services.analytics import WeatherAnalytics
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import atexit

app = Flask(__name__)
app.config.from_object(Config)

# Initialize services
weather_model = WeatherModel(Config.MONGODB_URI, Config.DATABASE_NAME)
data_ingester = WeatherDataIngester(weather_model)
analytics = WeatherAnalytics(weather_model)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Background scheduler for data collection
scheduler = BackgroundScheduler()

def scheduled_data_collection():
    """Collect weather data for predefined cities"""
    cities = ['New York', 'London', 'Tokyo', 'Sydney', 'Mumbai', 'Berlin']
    result = data_ingester.ingest_multiple_cities(cities)
    logger.info(f"Scheduled data collection completed. Inserted: {len(result) if result else 0} records")

# Schedule data collection every hour
scheduler.add_job(
    func=scheduled_data_collection,
    trigger="interval",
    hours=1,
    id='weather_data_collection'
)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template('dashboard.html')

@app.route('/api/weather/<city>')
def get_weather_data(city):
    """Get recent weather data for a city"""
    limit = request.args.get('limit', 100, type=int)
    data = weather_model.get_weather_by_location(city, limit)
    return jsonify(data)

@app.route('/api/analytics/temperature-trend/<city>')
def temperature_trend(city):
    """Get temperature trend analysis"""
    days = request.args.get('days', 30, type=int)
    analysis = analytics.temperature_trend_analysis(city, days)
    return jsonify(analysis)

@app.route('/api/analytics/weather-patterns/<city>')
def weather_patterns(city):
    """Get weather pattern analysis"""
    days = request.args.get('days', 90, type=int)
    analysis = analytics.weather_pattern_analysis(city, days)
    return jsonify(analysis)

@app.route('/api/analytics/extreme-weather/<city>')
def extreme_weather(city):
    """Get extreme weather events"""
    threshold = request.args.get('threshold', 95, type=int)
    analysis = analytics.extreme_weather_detection(city, threshold)
    return jsonify(analysis)

@app.route('/api/ingest/<city>')
def manual_ingest(city):
    """Manually trigger data ingestion for a city"""
    weather_data = data_ingester.fetch_current_weather(city)
    if weather_data:
        result = weather_model.insert_weather_data(weather_data)
        return jsonify({'success': True, 'inserted_id': str(result)})
    return jsonify({'success': False, 'error': 'Failed to fetch weather data'})

@app.route('/api/cities')
def get_cities():
    """Get list of cities with weather data"""
    pipeline = [
        {"$group": {"_id": "$location.city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    cities = list(weather_model.collection.aggregate(pipeline))
    return jsonify(cities)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
