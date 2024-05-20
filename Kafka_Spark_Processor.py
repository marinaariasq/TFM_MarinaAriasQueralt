import findspark
findspark.init()

from API_settings import HADOOP_HOME, JAVA_HOME, HADOOP_HOME_PATH
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType, ArrayType, TimestampType
import os

class Kafka_Spark_Processor:
    def __init__(self, app_name, kafka_bootstrap_servers, air_pollution_data_topic, weather_data_topic, configure_partitions=False):
        """
        Initialize the Kafka_Spark_Processor class.

        Args:
            app_name (str): The name of the Spark application.
            kafka_bootstrap_servers (str): The Kafka bootstrap servers.
            air_pollution_data_topic (str): The Kafka topic stablished for the ingestion of air pollution data.
            weather_data_topic (str): The Kafka topic stablished for the ingestion of weather data.
            configure_partitions (bool): Whether to configure shuffle partitions and default parallelism. (for machine learning)
        """

        # Set environment variables of Hadoop and Java for the correct execution of spark
        os.environ['HADOOP_HOME'] = HADOOP_HOME
        os.environ['JAVA_HOME'] = JAVA_HOME
        os.environ['PATH'] += os.pathsep + HADOOP_HOME_PATH
        os.environ['PATH'] += os.pathsep + os.environ['JAVA_HOME'] + '\\bin'

        spark_builder = SparkSession.builder \
            .master("local[*]") \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8") \

        if configure_partitions:
            spark_builder = spark_builder \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.default.parallelism", "200")

        self.spark_session = spark_builder.getOrCreate()    
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.air_pollution_data_topic = air_pollution_data_topic
        self.weather_data_topic = weather_data_topic


    def get_kafka_consumer(self, topic):
        """
        Stablish a Kafka consumer for an specified topic.

        Args:
            topic (str): The Kafka topic to subscribe to.

        Returns:
            DataFrame: A Spark DataFrame representing the Kafka stream.
        """
        return self.spark_session \
            .readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def parse_air_pollution_data(self, df, schema):
        """
        Parse raw air pollution data from a Kafka DataFrame.

        Args:
            df (DataFrame): The Spark DataFrame containing raw data from Kafka.
            schema (StructType): The schema defining the structure of the air pollution data.

        Returns:
            DataFrame: A Spark DataFrame of air pollution data.
        """
        return df.select(from_json(col("json_value"), schema).alias("data")) \
            .select(explode(col("data")).alias("unix_timestamp", "entries")) \
            .select(from_unixtime(col("unix_timestamp")).cast(TimestampType()).alias("timestamp"), explode(col("entries")).alias("airpollution_data"))

    def parse_weather_data(self, df, schema):
        """
        Parse raw weather data from a Kafka DataFrame.

        Args:
            df (DataFrame): The Spark DataFrame containing raw data from Kafka.
            schema (StructType): The schema defining the structure of the weather data.

        Returns:
            DataFrame: A Spark DataFrame of weather data.
        """
        return df.select(from_json(col("json_value"), schema).alias("data")) \
            .select(explode(col("data")).alias("unix_timestamp", "entries")) \
            .select(from_unixtime(col("unix_timestamp")).cast(TimestampType()).alias("timestamp"), explode(col("entries")).alias("weather_data"))   
    
    def get_air_pollution_schema(self):
        """
        Schema for air pollution data.

        Returns:
            StructType: The schema of incoming air pollution data.
        """
        return MapType(StringType(), ArrayType(StructType([
            StructField("location_id", StructType([
                StructField("location_name", StringType(), True),
                StructField("lon", DoubleType(), True),
                StructField("lat", DoubleType(), True)
            ]), True),
            StructField("air_pollution_info", StructType([
                StructField("main", StructType([
                    StructField("aqi", IntegerType(), True)
                ]), True),
                StructField("components", StructType([
                    StructField("co", DoubleType(), True),
                    StructField("no", DoubleType(), True),
                    StructField("no2", DoubleType(), True),
                    StructField("o3", DoubleType(), True),
                    StructField("so2", DoubleType(), True),
                    StructField("pm2_5", DoubleType(), True),
                    StructField("pm10", DoubleType(), True),
                    StructField("nh3", DoubleType(), True)
                ]), True)
            ]), True)
        ])))

    def get_weather_data_schema(self):
        """
        Schema for weather data.

        Returns:
            StructType: The schema of incoming weather data.
        """
        return MapType(StringType(), ArrayType(StructType([
            StructField("location_id", StringType(), True),
            StructField("weather_info", StructType([
                StructField("main", StructType([
                    StructField("temp", DoubleType(), True),
                    StructField("feels_like", DoubleType(), True),
                    StructField("pressure", IntegerType(), True),
                    StructField("humidity", IntegerType(), True),
                    StructField("temp_min", DoubleType(), True),
                    StructField("temp_max", DoubleType(), True)
                ]), True),
                StructField("wind", StructType([
                    StructField("speed", DoubleType(), True),
                    StructField("deg", IntegerType(), True),
                    StructField("gust", DoubleType(), True)
                ]), True),
                StructField("clouds", StructType([
                    StructField("all", IntegerType(), True)
                ]), True),
                StructField("weather", ArrayType(StructType([
                    StructField("id", IntegerType(), True),
                    StructField("main", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("icon", StringType(), True)
                ])), True)
            ]), True)
        ])))