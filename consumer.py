"""
Weather Data Consumer - Spark Streaming Consumer
Consumes from Kafka, processes streams, and stores in PostgreSQL
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, count,
    current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = 'weather-data'
POSTGRES_URL = os.getenv('POSTGRES_URL', 'jdbc:postgresql://localhost:5432/weather_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

# Kafka and PostgreSQL JARs - these will be auto-downloaded
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
POSTGRES_PACKAGE = "org.postgresql:postgresql:42.7.3"

# Define schema for incoming weather data
weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("cloudiness", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
])

class WeatherStreamProcessor:
    def __init__(self):
        """Initialize Spark Streaming session with proper configurations"""
        print("🔧 Initializing Spark Session...")
        print(f"   Downloading dependencies: {KAFKA_PACKAGE}")
        print(f"   This may take a few minutes on first run...")
        
        # Create Spark session with all necessary configurations
        self.spark = SparkSession.builder \
            .appName("WeatherStreamingPipeline") \
            .master("local[*]") \
            .config("spark.jars.packages", f"{KAFKA_PACKAGE},{POSTGRES_PACKAGE}") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.jars.ivy", os.path.expanduser("~/.ivy2")) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark Session initialized successfully!")
        print(f"   Spark Version: {self.spark.version}")
        print(f"   Master: {self.spark.sparkContext.master}")
        print()
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        print(f"📡 Connecting to Kafka broker: {KAFKA_BROKER}")
        print(f"   Topic: {KAFKA_TOPIC}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"✅ Connected to Kafka topic: {KAFKA_TOPIC}")
        
        # Parse JSON data
        weather_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), weather_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp")))
        
        return weather_df
    
    def process_stream(self, weather_df):
        """
        Process streaming data with windowed aggregations
        Lambda Architecture - Stream Processing Layer
        """
        print("⚙️  Setting up windowed aggregations (5-minute tumbling windows)")
        
        # Real-time aggregations (5-minute tumbling windows)
        windowed_stats = weather_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("city")
            ) \
            .agg(
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("wind_speed").alias("avg_wind_speed"),
                count("*").alias("record_count")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "city",
                "avg_temperature",
                "max_temperature",
                "min_temperature",
                "avg_humidity",
                "avg_wind_speed",
                "record_count"
            )
        
        print("✅ Windowed aggregations configured")
        return windowed_stats
    
    def write_to_postgres_raw(self, weather_df):
        """
        Write raw streaming data to PostgreSQL
        Lambda Architecture - Speed Layer (Raw data for serving layer)
        """
        print("💾 Configuring raw data stream to PostgreSQL...")
        
        def batch_write_raw(batch_df, batch_id):
            """Write batch to PostgreSQL - raw weather data"""
            if batch_df.count() > 0:
                try:
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", POSTGRES_URL) \
                        .option("dbtable", "weather_raw") \
                        .option("user", POSTGRES_USER) \
                        .option("password", POSTGRES_PASSWORD) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                    
                    count = batch_df.count()
                    print(f"📝 Batch {batch_id}: Wrote {count} raw records to PostgreSQL")
                except Exception as e:
                    print(f"❌ Error writing batch {batch_id}: {e}")
        
        query = weather_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(batch_write_raw) \
            .option("checkpointLocation", "/tmp/spark-checkpoint-raw") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("✅ Raw data stream started (writes every 30 seconds)")
        return query
    
    def write_to_postgres_aggregated(self, windowed_stats):
        """
        Write aggregated data to PostgreSQL
        Lambda Architecture - Batch Layer (Pre-computed views)
        """
        print("📊 Configuring aggregated data stream to PostgreSQL...")
        
        def batch_write_aggregated(batch_df, batch_id):
            """Write batch to PostgreSQL - aggregated statistics"""
            if batch_df.count() > 0:
                try:
                    # Create temp view for upsert
                    batch_df.createOrReplaceTempView("temp_aggregated")
                    
                    # Write to PostgreSQL
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", POSTGRES_URL) \
                        .option("dbtable", "weather_aggregated") \
                        .option("user", POSTGRES_USER) \
                        .option("password", POSTGRES_PASSWORD) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                    
                    count = batch_df.count()
                    print(f"📊 Batch {batch_id}: Wrote {count} aggregated records to PostgreSQL")
                    
                    # Show sample of aggregated data
                    batch_df.show(5, truncate=False)
                except Exception as e:
                    print(f"❌ Error writing aggregated batch {batch_id}: {e}")
        
        query = windowed_stats \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(batch_write_aggregated) \
            .option("checkpointLocation", "/tmp/spark-checkpoint-agg") \
            .trigger(processingTime='1 minute') \
            .start()
        
        print("✅ Aggregated data stream started (writes every 1 minute)")
        return query
    
    def write_to_console(self, df, name="Stream"):
        """Write stream to console for monitoring"""
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "5") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print(f"✅ Console output started for {name}")
        return query
    
    def run(self):
        """Main processing pipeline"""
        print("=" * 70)
        print("🚀 STARTING WEATHER STREAMING PIPELINE WITH SPARK")
        print("=" * 70)
        print()
        
        try:
            # Read from Kafka
            weather_stream = self.read_from_kafka()
            print()
            
            # Process stream (windowed aggregations)
            aggregated_stream = self.process_stream(weather_stream)
            print()
            
            # Write to multiple sinks
            print("📤 Starting output streams...")
            print("-" * 70)
            
            query_raw = self.write_to_postgres_raw(weather_stream)
            query_agg = self.write_to_postgres_aggregated(aggregated_stream)
            query_console = self.write_to_console(weather_stream, "Raw Weather Data")
            
            print()
            print("=" * 70)
            print("✅ ALL STREAMING QUERIES STARTED SUCCESSFULLY!")
            print("=" * 70)
            print()
            print("📊 Stream Information:")
            print(f"   - Raw data checkpoint: /tmp/spark-checkpoint-raw")
            print(f"   - Aggregated checkpoint: /tmp/spark-checkpoint-agg")
            print(f"   - Processing trigger: Every 30 seconds")
            print()
            print("🎯 Lambda Architecture Layers:")
            print("   - Speed Layer: Raw data → weather_raw table")
            print("   - Batch Layer: 5-min windows → weather_aggregated table")
            print("   - Serving Layer: Flask API queries both tables")
            print()
            print("=" * 70)
            print("📡 MONITORING STREAMS... Press Ctrl+C to stop")
            print("=" * 70)
            print()
            
            # Wait for termination
            query_raw.awaitTermination()
            
        except KeyboardInterrupt:
            print()
            print("=" * 70)
            print("⚠️  STOPPING STREAMS...")
            print("=" * 70)
            
            # Stop all queries
            for query in self.spark.streams.active:
                print(f"   Stopping query: {query.name}")
                query.stop()
            
            print()
            print("✅ All streams stopped gracefully")
            print("=" * 70)
            
        except Exception as e:
            print()
            print("=" * 70)
            print(f"❌ ERROR: {e}")
            print("=" * 70)
            import traceback
            traceback.print_exc()
            
        finally:
            # Stop Spark session
            print()
            print("🛑 Stopping Spark session...")
            self.spark.stop()
            print("✅ Spark session stopped")

def main():
    """Entry point"""
    print()
    print("=" * 70)
    print("         WEATHER DATA STREAMING PIPELINE - SPARK CONSUMER")
    print("=" * 70)
    print()
    
    # Check environment variables
    print("🔍 Configuration Check:")
    print(f"   Kafka Broker: {KAFKA_BROKER}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   PostgreSQL URL: {POSTGRES_URL}")
    print()
    
    processor = WeatherStreamProcessor()
    processor.run()

if __name__ == '__main__':
    main()