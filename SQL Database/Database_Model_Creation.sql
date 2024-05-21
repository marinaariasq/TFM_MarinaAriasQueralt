USE AirPollutionWeatherData
GO

-- Create the tables into the DB
CREATE TABLE Locations (
    location_id INT PRIMARY KEY IDENTITY(1,1),
    location_name NVARCHAR(255) UNIQUE,
    longitude FLOAT,
    latitude FLOAT);


CREATE TABLE AirPollutionData (
    air_pollution_data_id INT PRIMARY KEY IDENTITY(1,1),
    location_id INT,
    air_quality_index INT,
    co FLOAT,
    no FLOAT,
    no2 FLOAT,
    o3 FLOAT,
    so2 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    nh3 FLOAT,
    timestamp_measurement DATETIME
    FOREIGN KEY (location_id) REFERENCES Locations(location_id));


CREATE TABLE WeatherData (
    weather_data_id INT PRIMARY KEY IDENTITY(1,1),
    location_id INT,
    temperature FLOAT,
    thermal_sensation FLOAT,
    pressure INT,
    humidity INT,
    minimum_temperature FLOAT,
    maximum_temperature FLOAT,
    wind_speed FLOAT,
    wind_direction INT,
    wind_gust FLOAT,
    cloudiness_percentage INT,
    weather_description NVARCHAR(255),
    timestamp_measurement DATETIME,
    FOREIGN KEY (location_id) REFERENCES Locations(location_id));


-- Check the tables
select * from AirPollutionData ORDER BY timestamp_measurement, location_id;
select * from Locations;
select * from WeatherData ORDER BY timestamp_measurement, location_id;



