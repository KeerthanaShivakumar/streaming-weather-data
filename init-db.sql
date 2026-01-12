-- Database initialization script

-- Raw weather data table (Speed Layer in Lambda Architecture)
CREATE TABLE IF NOT EXISTS weather_raw (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    feels_like DOUBLE PRECISION,
    humidity INTEGER,
    pressure INTEGER,
    weather_condition VARCHAR(50),
    weather_description VARCHAR(200),
    wind_speed DOUBLE PRECISION,
    cloudiness INTEGER,
    visibility INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for efficient queries
CREATE INDEX idx_weather_city_timestamp ON weather_raw(city, timestamp DESC);
CREATE INDEX idx_weather_timestamp ON weather_raw(timestamp DESC);

-- Aggregated statistics table (Batch Layer in Lambda Architecture)
CREATE TABLE IF NOT EXISTS weather_aggregated (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city VARCHAR(100) NOT NULL,
    avg_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_wind_speed DOUBLE PRECISION,
    record_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for aggregated data
CREATE INDEX idx_weather_agg_city_window ON weather_aggregated(city, window_start DESC);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Insert sample data for testing (optional)
-- This helps verify the dashboard works even before streaming starts
INSERT INTO weather_raw (city, latitude, longitude, timestamp, temperature, feels_like, 
                         humidity, pressure, weather_condition, weather_description, 
                         wind_speed, cloudiness, visibility)
VALUES 
    ('Geneva', 46.2044, 6.1432, NOW(), 15.5, 14.2, 75, 1013, 'Clouds', 'scattered clouds', 3.5, 40, 10000),
    ('Zurich', 47.3769, 8.5417, NOW(), 16.2, 15.1, 70, 1015, 'Clear', 'clear sky', 2.8, 10, 10000);