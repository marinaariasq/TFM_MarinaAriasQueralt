from API_data_extractor import OpenWheatherAPI
from API_settings import API_acces_token

import pandas as pd
import datetime
import json

# Stablish start date and end date of the exported historical data (one year before current data)
Year = 2024
Month = 1
Day = 1

start_date = datetime.datetime(Year, Month, Day)
start_date_unix = int(start_date.timestamp())

end_date = start_date + datetime.timedelta(weeks=9)
print(end_date)
end_date_unix = int(end_date.timestamp())


# Stablish the locations available in the OpenWheather API that are going to be used to define Barcelona's municipe
path_barcelona_locations = r'.\saved_data_files_for_extraction\barcelona_locations.txt'
city_list_json = r'.\saved_data_files_for_extraction\city.list.json'

city_list_data = pd.read_json(city_list_json)
spain_locations = city_list_data[city_list_data['country']=='ES']

with open(path_barcelona_locations, 'r', encoding='utf-8') as file:
    barcelona_locations = file.read()
    barcelona_locations = barcelona_locations.replace('"', '')
    barcelona_locations = barcelona_locations.split(', ') 

barcelona_municipe_locations = spain_locations[spain_locations['name'].isin(barcelona_locations)].copy()
barcelona_municipe_locations.loc[:, 'lon'] = barcelona_municipe_locations['coord'].apply(lambda x: x['lon'])
barcelona_municipe_locations.loc[:, 'lat'] = barcelona_municipe_locations['coord'].apply(lambda x: x['lat'])
barcelona_municipe_locations = barcelona_municipe_locations[["name", "id", "lon", "lat"]].reset_index()
barcelona_municipe_locations.to_csv("barcelona_municipe_locations", index=False)


# Initialize the class OpenWheatherAPI to make the data extractions from the OpenWheatherAPI
API_Data_Extractor= OpenWheatherAPI(API_acces_token, start_date_unix, end_date_unix)

# Extract historic data from AirPollution API

all_pollution_data = {}
for index, row in barcelona_municipe_locations.iterrows():
    air_pollution_data = API_Data_Extractor.make_api_request_polution_data(row['lon'], row['lat'])
    print(row['name'])
    if air_pollution_data:
        all_pollution_data[row['name']] = air_pollution_data

with open('all_air_pollution_data_2024.json', 'w') as outfile:
    print("Write the JSON file.")
    json.dump(all_pollution_data, outfile)
    print("The JSON file with air pollution data grouped by location has been created and saved.")



# Extract historic data from Weather API

all_weather_data = {}

for index, row in barcelona_municipe_locations.iterrows():
    print(row['name'])
    print(index)
    start_date = datetime.datetime(Year, Month, Day)
    weather_data_list = []

    for i in range(9):  # Assuming 51 weeks
        timestamp = int(start_date.timestamp())
        weather_data = API_Data_Extractor.make_api_request_wheather_data(row['lon'], row['lat'], timestamp)
        if weather_data and 'list' in weather_data:
            for item in weather_data['list']:
                dt = item.get('dt')
                if dt not in {x.get('dt') for x in weather_data_list}:
                    weather_data_list.append(item)
        start_date += datetime.timedelta(days=7)

    if weather_data_list:
        all_weather_data[row['name']] = {"list": weather_data_list}


with open('all_wheather_data_2024.json', 'w') as outfile:
    print("Write the JSON file.")
    json.dump(all_weather_data, outfile)
    print("The JSON file with wheather data grouped by location has been created and saved.")

