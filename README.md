# TFM Implementation

This code is part of the practical implementation of the TFM (Final Master's Thesis) titled "Forecasting Air Quality for the Province of Barcelona: Integrating Simulated Streaming with Advanced Data Processing Techniques" by Marina Arias Queralt, for the Master’s Degree in Data Science at the Open University of Catalonia (UOC).

## Overview
The project consists of different scripts grouped into "real-time data ingestion simulation" and "real-time data processing."

### Real-time Data Ingestion Simulation
+ **OpenWeatherData_extraction.py:** Extracts data from the OpenWeatherMap platform (History API and Air Pollution API) and saves them into two JSON files.

    + **API_data_extractor.py:** Defines a class that interacts with the OpenWeatherMap API to retrieve historical air pollution and weather data for a given location. Used in OpenWeatherData_extraction.py.

+ **Streaming_Simulation.py:** Reads the generated JSON files and ingests their data into the corresponding Kafka topics.

    +  **Kafka_Data_Manager.py:** Defines a class that facilitates interaction with a Kafka server for managing topics and sending simulated streaming data. Used in Streaming_Simulation.py.

### Real-time Data Processing
+ **Stream_Processing_to_Datalake.py:** Processes the data ingested into the Kafka topics and stores it in an SQL Server data lake.
+ **Stream_Processing_machine_learning.py:** Processes the data ingested into the Kafka topics and uses it to train a machine learning model (Random Forest) to predict air quality based on input data.
    + **Kafka_Spark_Processor.py:** Defines a class that facilitates the processing of streaming data from the Kafka topics using Apache Spark. Used in Stream_Processing_machine_learning.py and Stream_Processing_to_Datalake.py.


## Installation

1. Clone the repository:

     > git clone https://github.com/marinaariasq/TFM_MarinaAriasQueralt.git 

3. Install the required Python packages:

     > pip install -r requirements.txt

## How to use

1. Extract data from the OpenWeatherMap APIS:

Open a terminal and run: 
    
    > python OpenWeatherData_extraction.py

2. Real-Time Data Processing:

Once all the data from OpenWeatherMap is extracted, the real-time data processing begins. But first, initialize Zookeeper and the Kafka server:

    >.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
    >.\bin\windows\kafka-server-start.bat .\config\server.properties    
    
Now, open two terminals and run the following scripts simultaneously:
    
    > python Stream_Processing_Machine_Learning.py
    > python Stream_Processing_to_Datalake.py

3. Data Ingestion Simulation:
   
While the scripts Stream_Processing_Machine_Learning.py and Stream_Processing_to_Datalake.py are running, open another terminal and run:

    > python Streaming_Simulation.py

## Settings

To run this project, you will need an API key and user credentials for the OpenWeatherMap APIs, as well as credentials for the SQL Server database.
