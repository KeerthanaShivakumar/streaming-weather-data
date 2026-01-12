"""
Weather Dashboard - Flask API
Serves real-time and historical weather data from PostgreSQL
"""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
import psycopg
from psycopg.rows import dict_row
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# PostgreSQL Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'dbname': os.getenv('POSTGRES_DB', 'weather_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

def get_db_connection():
    """Create database connection"""
    return psycopg.connect(
        **DB_CONFIG,
        row_factory=dict_row
    )

@app.route('/')
def index():
    """Render dashboard"""
    return render_template('weather_dashboard.html')

@app.route('/api/current')
def get_current_weather():
    """Get latest weather data for all cities"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()       
        # Get most recent record for each city
        query = """
            SELECT DISTINCT ON (city)
                city, temperature, feels_like, humidity, pressure,
                weather_condition, weather_description, wind_speed,
                cloudiness, visibility, timestamp
            FROM weather_raw
            ORDER BY city, timestamp DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert timestamp to ISO format
        for row in results:
            if row['timestamp']:
                row['timestamp'] = row['timestamp'].isoformat()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/<city>')
def get_city_history(city):
    """Get historical data for specific city (last 24 hours)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get last 24 hours of data
        query = """
            SELECT 
                city, temperature, humidity, wind_speed,
                weather_condition, timestamp
            FROM weather_raw
            WHERE city = %s 
                AND timestamp > NOW() - INTERVAL '24 hours'
            ORDER BY timestamp ASC
        """
        
        cursor.execute(query, (city,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert timestamps
        for row in results:
            if row['timestamp']:
                row['timestamp'] = row['timestamp'].isoformat()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aggregated/<city>')
def get_aggregated_stats(city):
    """Get aggregated statistics for specific city"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                window_start, window_end, city,
                avg_temperature, max_temperature, min_temperature,
                avg_humidity, avg_wind_speed, record_count
            FROM weather_aggregated
            WHERE city = %s 
                AND window_start > NOW() - INTERVAL '24 hours'
            ORDER BY window_start ASC
        """
        
        cursor.execute(query, (city,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert timestamps
        for row in results:
            if row['window_start']:
                row['window_start'] = row['window_start'].isoformat()
            if row['window_end']:
                row['window_end'] = row['window_end'].isoformat()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_overall_stats():
    """Get overall system statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) as total FROM weather_raw")
        total_records = cursor.fetchone()['total']
        
        # Cities count
        cursor.execute("SELECT COUNT(DISTINCT city) as cities FROM weather_raw")
        cities_count = cursor.fetchone()['cities']
        
        # Latest update time
        cursor.execute("SELECT MAX(timestamp) as latest FROM weather_raw")
        latest_update = cursor.fetchone()['latest']
        
        # Records in last hour
        cursor.execute("""
            SELECT COUNT(*) as recent 
            FROM weather_raw 
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """)
        recent_records = cursor.fetchone()['recent']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'cities_monitored': cities_count,
            'latest_update': latest_update.isoformat() if latest_update else None,
            'records_last_hour': recent_records
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)