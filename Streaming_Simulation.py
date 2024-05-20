import datetime
import time
import json
from Kafka_Data_Manager import KafkaDataManager

#######################################################################################################################
# Define the functions to prepapre data to the correct format for the ingestion
def process_air_pollution_data_for_ingestion(data):
    """
    Processes air pollution data from the initial saved version extracted from the OpenWeatherMap API and adapts the format for ingestion into Kafka.

    Args:
        data (dict): The air pollution data, structured as a dictionary where keys are location names and values are dictionaries containing coordinates and a list of pollution readings.
    
    Returns:
        dict: A dictionary where keys are timestamps and values are lists of processed air pollution entries. Each entry includes location information and air pollution details.
    """
    processed_data = {}
    for location, info in data.items():
        location_id = {
            'location_name': location,
            'lon': info['coord']['lon'],
            'lat': info['coord']['lat']
        }
        for item in info['list']:
            dt = item['dt']
            entry = {
                'location_id': location_id,
                'air_pollution_info': {'main': item['main'], 'components': item['components']}
            }
            if dt in processed_data:
                processed_data[dt].append(entry)
            else:
                processed_data[dt] = [entry]
    return processed_data

def process_weather_data_for_ingestion(data):
    """
    Processes weather data from the initial saved version extracted from the OpenWeatherMap API and adapts the format for ingestion into Kafka.

    Args:
        data (dict): The weather data, structured as a dictionary where keys are location names and values are dictionaries containing lists of weather readings.
    
    Returns:
        dict: A dictionary where keys are timestamps and values are lists of processed weather entries. Each entry includes location information and weather details.
    """
    processed_data = {}
    for location, info in data.items():
        for item in info['list']:
            dt = item['dt']
            entry = {
                'location_id': location,
                'weather_info': {
                    'main': item['main'], 
                    'wind': item['wind'],
                    'clouds': item['clouds'],
                    'weather': item['weather']
                }
            }
            if dt in processed_data:
                processed_data[dt].append(entry)
            else:
                processed_data[dt] = [entry]
    return processed_data

#######################################################################################################################
# Load the json files with the downloaded data from the API and convert it to the corrent format for ingestion

# Air pollution data
with open('all_air_pollution_data_2024.json', 'r') as file:
    # Load the JSON SA into a Python dictionary
    air_pollution_data = json.load(file)

dict_dt_ready_to_stream_air_pollution = process_air_pollution_data_for_ingestion(air_pollution_data)

# Wheather data
with open('all_wheather_data_2024.json', 'r') as file:
    # Load the JSON SA into a Python dictionary
    wheather_data = json.load(file)

dict_dt_ready_to_stream_weather = process_weather_data_for_ingestion(wheather_data)


#######################################################################################################################
# Start de ingestion of the data into the kafka server

# First create the topics where the data will be stored in Kafka
KafkaClass = KafkaDataManager()
KafkaClass.create_kafka_topics(['air_pollution_data_topic_2024', 'weather_data_topic_2024'])

# Send data to kafka server
dict_datetimes = sorted(set(dict_dt_ready_to_stream_weather.keys()) | set(dict_dt_ready_to_stream_air_pollution.keys()))

p=0
for dt in dict_datetimes[:10]:
    p +=1
    print(f"{p}/{len(dict_datetimes)}")
    air_pollution_data = dict_dt_ready_to_stream_air_pollution.get(dt, [])
    weather_data = dict_dt_ready_to_stream_weather.get(dt, [])
    
    # Debugging prints (Check data to be ingested into kafka)
    print(f"Sending air pollution data for timestamp {dt}: {air_pollution_data}")
    print(f"Sending weather data for timestamp {dt}: {weather_data}")

    try:
        KafkaClass.send_data_to_kafka('air_pollution_data_topic_2024', {dt: air_pollution_data})
        KafkaClass.send_data_to_kafka('weather_data_topic_2024', {dt:weather_data})
        print("Sended data")
        
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

    time.sleep(3)
 
print("All data ingested")
