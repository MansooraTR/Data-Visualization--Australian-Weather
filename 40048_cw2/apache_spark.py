from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Australian Weather Data Processing") \
    .getOrCreate()

# Load weather data from CSV file into DataFrame
weather_df = spark.read.csv("Australia_weather_dataset.csv", header=True, inferSchema=True)

numerical_cols = weather_df.select_dtypes(include=['float64', 'int64']).columns
weather_df[numerical_cols] = weather_df[numerical_cols].fillna(weather_df[numerical_cols].mean())

# Show the schema of the DataFrame
weather_df.printSchema()

# Show the first few rows of the DataFrame
weather_df.show()

# Perform data processing or analysis here (e.g., filtering, aggregations, etc.)
ordered_weather_df = weather_df.orderBy("Humidity9am")

# Show the ordered DataFrame
ordered_weather_df.show()

# Stop the Spark session
spark.stop()
