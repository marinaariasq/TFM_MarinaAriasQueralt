import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics
from Kafka_Spark_Processor import Kafka_Spark_Processor  
import shutil
import pandas as pd


# Remove previous checkpoints to avoid errors
checkpoint_dir = "D:\\TFM_MODELS_ML\\checkpoints"
shutil.rmtree(checkpoint_dir, ignore_errors=True)

# Initialize Kafka_Spark_Processor class
SparkProcessor = Kafka_Spark_Processor(
    app_name="Kafka_Spark_OpenWheather_ML",
    kafka_bootstrap_servers="marina-laptop:9092",
    air_pollution_data_topic="air_pollution_data_topic_2024",
    weather_data_topic="weather_data_topic_2024",
    configure_partitions = True
)

# Define kafka consumers for each topic (Weather data and Air Pollution data)
kafka_consumer_air_pollution = SparkProcessor.get_kafka_consumer(SparkProcessor.air_pollution_data_topic)
kafka_consumer_weather = SparkProcessor.get_kafka_consumer(SparkProcessor.weather_data_topic)

# Convert JSON data from Kafka topics into a Spark DataFrame
df_Air_Pollution = kafka_consumer_air_pollution.selectExpr("CAST(value AS STRING) as json_value")
df_Weather = kafka_consumer_weather.selectExpr("CAST(value AS STRING) as json_value")

# Define the schema of the data incoming from each Kafka topic
schema_Air_Pollution = SparkProcessor.get_air_pollution_schema()
schema_Weather = SparkProcessor.get_weather_data_schema()

# Air Pollution Data
df_Air_Pollution_parsed = SparkProcessor.parse_air_pollution_data(df_Air_Pollution, schema_Air_Pollution)
df_Air_Pollution_selected = df_Air_Pollution_parsed.select(
    col("airpollution_data.location_id.location_name").alias("location_name_air_pollution"),
    col("airpollution_data.location_id.lon").alias("longitude"),
    col("airpollution_data.location_id.lat").alias("latitude"),
    col("timestamp").alias("timestamp_air_pollution"),
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

# Weather data
df_Weather_Parsed = SparkProcessor.parse_weather_data(df_Weather, schema_Weather)
df_Weather_selected = df_Weather_Parsed.select(
    col("timestamp").alias("timestamp_weather"),
    col("weather_data.location_id").alias("location_name_weather"),
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
    col("weather_data.weather_info.weather.description").getItem(0).alias("weather_description")
).distinct()

# Join Air Pollution Data and Weather Data by timestamp and location_name
df_joined = df_Air_Pollution_selected.join(df_Weather_selected,
                                           (df_Air_Pollution_selected.timestamp_air_pollution == df_Weather_selected.timestamp_weather) & 
                                           (df_Air_Pollution_selected.location_name_air_pollution == df_Weather_selected.location_name_weather),
                                           "inner")


# Select only the relevant columns 
df_unified = df_joined.select(
    col("timestamp_air_pollution").alias("timestamp_measurement"),
    col("location_name_air_pollution").alias("location_name"),
    col("longitude"), col("latitude"),
    col("co"), col("no"), col("no2"), col("o3"), col("so2"), col("pm2_5"), col("pm10"), col("nh3"),
    col("temperature"), col("thermal_sensation"), col("pressure"), col("humidity"), col("minimum_temperature"), col("maximum_temperature"), col("wind_speed"), col("wind_direction"), col("wind_gust"), col("cloudiness_percentage"),
    col("weather_description"), col("air_quality_index")
).distinct()

# Drop records with nulls
df_unified = df_unified.dropna()

# Preprocessing of the data in order to apply a random forest
assembler = VectorAssembler(inputCols=["longitude", "latitude", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", 
                                       "temperature", "thermal_sensation", "pressure", "humidity", "minimum_temperature", 
                                       "maximum_temperature", "wind_speed", "wind_direction", "wind_gust", "cloudiness_percentage"],
                            outputCol="features")

# Scale caractheristics
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# Stablish the machine learning model to apply
random_forest_model = RandomForestRegressor(featuresCol="scaled_features", labelCol="air_quality_index")

# Create the pipeline
pipeline = Pipeline(stages=[assembler, scaler, random_forest_model])

# Generate the evaluators to check the performance of the random forest model
evaluator_rmse = RegressionEvaluator(labelCol="air_quality_index", predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol="air_quality_index", predictionCol="prediction", metricName="mae")

def process_batch(batch_df, batch_id):

    # Show current unified dataframe , to confirm that there is an income of data
    batch_df.show(n=5, truncate=False)
    
    # Show the diferent numerical categories in 'air_quality_index'
    distinct_categories = batch_df.select("air_quality_index").distinct().collect()
    distinct_values = [row["air_quality_index"] for row in distinct_categories]
    print(f"\n Distinct categories in 'air_quality_index': {distinct_values} \n")
    print(f"\n Number of distinct categories: {len(distinct_values)} \n")

    # Obtain test and train datasets
    train_data, test_data = batch_df.randomSplit([0.8, 0.2])
       
    if train_data.isEmpty():
        print(f"Batch ID {batch_id}: Training data is empty. Skipping this batch.")
        return
    
    # Train the model 
    model = pipeline.fit(train_data)
    
    # Make predictions of the test dataset
    predictions = model.transform(test_data)
    
    # Evaluate the model
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")
    print(f"Mean Absolute Error (MAE) on test data = {mae}")
    
    # Round the predictions (to obtain the expected categories)
    predictions = predictions.withColumn("rounded_prediction", col("prediction").cast(IntegerType()))

    # Show predictions VS expected values
    predictions.select("rounded_prediction", "air_quality_index").show()
    
    # Get the Confusion Matrix and the Accuracy to confirm the quality of the machine learning model
    prediction_and_labels = predictions.select("rounded_prediction", "air_quality_index").rdd.map(lambda row: (float(row[0]), float(row[1])))
    metrics = MulticlassMetrics(prediction_and_labels)
    
    print("Confusion Matrix:")
    print(metrics.confusionMatrix().toArray())

    accuracy = metrics.accuracy
    print(f"Accuracy: {accuracy}")

    # Save the model
    metrics_data = {
        "batch_id": [batch_id],
        "rmse": [rmse],
        "mae": [mae],
        "accuracy": [accuracy]
    }
    metrics_df = pd.DataFrame(metrics_data)
    metrics_file = "D:\\TFM_MODELS_ML\\metrics.csv"
    metrics_df.to_csv(metrics_file, mode='a', header=not pd.io.common.file_exists(metrics_file), index=False)

    model_path = f"D:\\TFM_MODELS_ML\\{batch_id}"
    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")


# Generate the machine learning model with the incoming data from kafka topics
query = df_unified.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "D:\\TFM_MODELS_ML\\checkpoints") \
    .start()

query.awaitTermination()
