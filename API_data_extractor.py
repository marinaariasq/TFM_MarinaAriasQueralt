import requests
import pandas as pd


class OpenWheatherAPI:
    
    def __init__(self, api_key, start_date, end_date):
        """
        Initializes the OpenWeatherAPI with the provided API key, start date, and end date.

        Args:
            api_key (str): The API key for accessing the OpenWeatherMap API.
            start_date (int): The start date for the data retrieval period (Unix timestamp).
            end_date (int): The end date for the data retrieval period (Unix timestamp).
        """
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date

    def make_api_request_polution_data(self, longitude, latitude):
        """
        Makes an API request to retrieve historical air pollution data for a specific location.

        Args:
            longitude (float): The longitude of the location.
            latitude (float): The latitude of the location.

        Returns:
            dict or None: The JSON response from the API if the request is successful, None otherwise.
        """
                
        # Construct the API URL
        URL_AirPollution_API = (
            f"http://api.openweathermap.org/data/2.5/air_pollution/history"
            f"?lat={latitude}&lon={longitude}"
            f"&start={self.start_date}&end={self.end_date}"
            f"&appid={self.api_key}"
        )
        
        # Make the API request
        response = requests.get(URL_AirPollution_API)
        
        # Extract information from the API request
        if response.status_code == 200:
            size_in_bytes = len(response.content)
            print(f"The size of the API response is: {size_in_bytes} bytes \n")
            return response.json() 
        
        else:
            print(f"Error: {response.status_code}")
            return None     
        
    def make_api_request_wheather_data(self, longitude, latitude, first_start_date):
        """
        Makes an API request to retrieve historical weather data for a specific location.

        Args:
            longitude (float): The longitude of the location.
            latitude (float): The latitude of the location.
            first_start_date (int): The start date for the initial data retrieval period (Unix timestamp).

        Returns:
            dict or None: The JSON response from the API if the request is successful, None otherwise.
        """
        # Construct the API URL
        URL_Weather_API = (
            f"http://history.openweathermap.org/data/2.5/history/city"
            f"?lat={latitude}&lon={longitude}"
            f"&type=hour&start={first_start_date}&end={self.end_date}"
            f"&appid={self.api_key}"
        )

        # Make the API request
        response = requests.get(URL_Weather_API)
        
        # Extract information from the API request
        if response.status_code == 200:
            size_in_bytes = len(response.content)
            print(f"The size of the API response is: {size_in_bytes} bytes \n")
            return response.json() 
        
        else:
            print(f"Error: {response.status_code}")
            return None     
        
