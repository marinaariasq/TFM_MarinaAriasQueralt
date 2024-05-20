from Kafka_Spark_Processor import Kafka_Spark_Processor
from API_settings import *
from pyspark.sql.functions import from_json, explode, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType, ArrayType, TimestampType

import pyodbc
import threading
import time


# Initialize Kafka_Spark_Processor class
SparkProcessor = Kafka_Spark_Processor( app_name="Kafka_Spark_SQLSERVER_OpenWheather",
                                       kafka_bootstrap_servers="marina-laptop:9092",
                                       air_pollution_data_topic="air_pollution_data_topic_2024",
                                       weather_data_topic="weather_data_topic_2024",
                                       configure_partitions = False)


# Define kafka consumers for each topic (Weather data and Air Pollution data)
kafka_consumer_air_pollution = SparkProcessor.get_kafka_consumer(SparkProcessor.air_pollution_data_topic)
kafka_consumer_weather = SparkProcessor.get_kafka_consumer(SparkProcessor.weather_data_topic)


# Convert JSON data from Kafka topics into a Spark DataFrame
df_Air_Pollution = kafka_consumer_air_pollution.selectExpr("CAST(value AS STRING) as json_value")
df_Weather_Data = kafka_consumer_weather.selectExpr("CAST(value AS STRING) as json_value")

# Define the schema of the data incoming from each Kafka topic
schema_Air_Pollution = SparkProcessor.get_air_pollution_schema()
schema_Weather_Data = SparkProcessor.get_weather_data_schema()


# Generate a global lock to serialize access to the SQL database (to avoid interblocking between transactions)
db_lock = threading.Lock()

# Establish a timeout to stop the Spark processing if no data ingestion into our Kafka topics is detected in 2 minutes
timeout = 120 

def write_air_pollution_to_db(df, batch_id):
    """
    Write the data from "air_pollution_data_topic" into the SQL database.

    Args:
        df (DataFrame): The Spark DataFrame containing air pollution data.
        batch_id (int): The batch ID assigned during streaming processing in Apache Spark
    """
    max_retries = 5
    retry_delay = 2  

    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connector_string_DB)
            cursor = conn.cursor()

            df_Air_Pollution_parsed = SparkProcessor.parse_air_pollution_data(df, schema_Air_Pollution)

            df_Air_Pollution_selected = df_Air_Pollution_parsed.select(
                col("airpollution_data.location_id.location_name").alias("location_name"),
                col("airpollution_data.location_id.lon").alias("longitude"),
                col("airpollution_data.location_id.lat").alias("latitude"),
                col("timestamp").alias("timestamp_measurement"),
                col("airpollution_data.air_pollution_info.main.aqi").alias("air_quality_index"),
                col("airpollution_data.air_pollution_info.components.co").alias("co"),
                col("airpollution_data.air_pollution_info.components.no").alias("no"),
                col("airpollution_data.air_pollution_info.components.no2").alias("no2"),
                col("airpollution_data.air_pollution_info.components.o3").alias("o3"),
                col("airpollution_data.air_pollution_info.components.so2").alias("so2"),
                col("airpollution_data.air_pollution_info.components.pm2_5").alias("pm2_5"),
                col("airpollution_data.air_pollution_info.components.pm10").alias("pm10"),
                col("airpollution_data.air_pollution_info.components.nh3").alias("nh3")
            ).distinct()

            # Show through console the data from kafka to be inserted into the SQL Tables
            df_Air_Pollution_selected.show(truncate=False)

            with db_lock:
                for row in df_Air_Pollution_selected.collect():
                    parameters_location_table = (row.location_name, row.location_name, row.longitude, row.latitude)
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM Locations WHERE location_name = ?)
                        BEGIN
                            INSERT INTO Locations (location_name, longitude, latitude) VALUES (?, ?, ?)
                        END""", parameters_location_table)

                    cursor.execute("SELECT location_id FROM Locations WHERE location_name = ?", row.location_name)
                    location_id = cursor.fetchone()[0]

                    parameters_AirPollutionData_table = (location_id, row.timestamp_measurement, location_id, row.air_quality_index, row.co, row.no, row.no2, row.o3, row.so2, row.pm2_5, row.pm10, row.nh3, row.timestamp_measurement)
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM AirPollutionData WHERE location_id = ? AND timestamp_measurement = ?)
                        BEGIN
                            INSERT INTO AirPollutionData (location_id, air_quality_index, co, no, no2, o3, so2, pm2_5, pm10, nh3, timestamp_measurement)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        END""", parameters_AirPollutionData_table)

                conn.commit()
                print("Air pollution data added succesfully into the DB. \n")
                cursor.close()
                conn.close()
            break 

        except pyodbc.Error as e:
            if e.args[0] == '40001':  # Code that references interlocking between SQL transactions in the database (possible when using threads)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay) 
                else:
                    print(f"Failed transaction after {max_retries} attempts due to interlocking.")
                    raise
            else:
                print("Error while doing the transaction:", e)
                raise

def write_weather_to_db(df, batch_id):    
    """
    Write the data from "weather_data_topic" into the SQL database.

    Args:
        df (DataFrame): The Spark DataFrame containing weather data.
        batch_id (int): The batch ID assigned during streaming processing in Apache Spark
    """
    max_retries = 5
    retry_delay = 2  

    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connector_string_DB)
            cursor = conn.cursor()

            df_Weather_Parsed = SparkProcessor.parse_weather_data(df, schema_Weather_Data)

            df_Weather_selected = df_Weather_Parsed.select(
                col("timestamp").alias("timestamp_measurement"),
                col("weather_data.location_id").alias("location_name"),
                col("weather_data.weather_info.main.temp").alias("temperature"),
                col("weather_data.weather_info.main.feels_like").alias("thermal_sensation"),
                col("weather_data.weather_info.main.pressure"),
                col("weather_data.weather_info.main.humidity"),
                col("weather_data.weather_info.main.temp_min").alias("minimum_temperature"),
                col("weather_data.weather_info.main.temp_max").alias("maximum_temperature"),
                col("weather_data.weather_info.wind.speed").alias("wind_speed"),
                col("weather_data.weather_info.wind.deg").alias("wind_direction"),
                col("weather_data.weather_info.wind.gust").alias("wind_gust"),
                col("weather_data.weather_info.clouds.all").alias("cloudiness_percentage"),
                col("weather_data.weather_info.weather.description").alias("weather_description")
            ).distinct()

            # Show through console the data from kafka to be inserted into the SQL Tables
            df_Weather_selected.show(truncate=False)

            with db_lock:
                for row in df_Weather_selected.collect():
                    parameters_location_table = (row.location_name, row.location_name, None, None)
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM Locations WHERE location_name = ?)
                        BEGIN
                            INSERT INTO Locations (location_name, longitude, latitude) VALUES (?, ?, ?)
                        END
                    """, parameters_location_table)

                    cursor.execute("SELECT location_id FROM Locations WHERE location_name = ?", row.location_name)
                    location_id = cursor.fetchone()[0]

                    parameters_WeatherData_table = (location_id,  row.timestamp_measurement, location_id, row.temperature, row.thermal_sensation, row.pressure, row.humidity, row.minimum_temperature, row.maximum_temperature, row.wind_speed, row.wind_direction,
                                                    row.wind_gust, row.cloudiness_percentage, row.weather_description[0], row.timestamp_measurement)
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM WeatherData WHERE location_id = ? AND timestamp_measurement = ?)
                        BEGIN
                            INSERT INTO WeatherData (location_id, temperature, thermal_sensation, pressure, humidity, minimum_temperature, maximum_temperature, wind_speed, wind_direction, wind_gust, cloudiness_percentage, weather_description, timestamp_measurement)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        END""", parameters_WeatherData_table)

                conn.commit()
                print("Weather data added succesfully into the DB. \n")
                cursor.close()
                conn.close()
            break 
        
        except pyodbc.Error as e:
            if e.args[0] == '40001': 
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)  
                else:
                    print(f"Failed transaction after {max_retries} attempts due to interlocking.")
                    raise
            else:
                print("Error while doing the transaction:", e)
                raise

df_Air_Pollution.printSchema()
df_Weather_Data.printSchema()

# Check the last time there was an incoming data from Kafka
last_activity_time = time.time()

def manage_air_pollution_stream():
    """
    Sets up a streaming query to read air pollution data from Kafka, processes it, and writes it to the SQL database using the `write_air_pollution_to_db` function. 
    This function continuously monitors the stream and stops the processing if no new data is received within a specified timeout.
    """
    global last_activity_time
    query_air_pollution = df_Air_Pollution.writeStream \
        .foreachBatch(write_air_pollution_to_db) \
        .outputMode("append") \
        .start()
    
    while True:  # Stop the Spark processing if there is no incoming data from Air_Pollution_Topic
        if query_air_pollution.status['isDataAvailable']:
            last_activity_time = time.time()
        if time.time() - last_activity_time > timeout:
            query_air_pollution.stop()
            break
        time.sleep(1)

def manage_weather_data_stream():
    """
    Sets up a streaming query to read weather data from Kafka, processes it, and writes it to the SQL database using the `write_weather_to_db` function. 
    This function continuously monitors the stream and stops the processing if no new data is received within a specified timeout.
    """
    global last_activity_time
    query_weather_data = df_Weather_Data.writeStream \
        .foreachBatch(write_weather_to_db) \
        .outputMode("append") \
        .start()

    while True: # Stop the Spark processing if there is no incoming data from Weather_Topic
        if query_weather_data.status['isDataAvailable']:
            last_activity_time = time.time()
        if time.time() - last_activity_time > timeout:
            query_weather_data.stop()
            break
        time.sleep(1)

# Create the threads and launch them to process the incoming data from both data topics at the same time
thread_air_pollution = threading.Thread(target=manage_air_pollution_stream)
thread_weather_data = threading.Thread(target=manage_weather_data_stream)

thread_air_pollution.start()
thread_weather_data.start()

thread_air_pollution.join()
thread_weather_data.join()

print("No more incoming data from both topics. Processing finalized.")