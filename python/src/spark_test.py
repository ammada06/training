from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Basic PySpark Example") \
    .getOrCreate()

# Create a list of dictionaries to act as our data
data = [
    {"name": "Alice", "age": 30, "city": "New York"},
    {"name": "Bob", "age": 25, "city": "San Francisco"},
    {"name": "Cathy", "age": 27, "city": "Los Angeles"}
]

# Create a DataFrame from the list of dictionaries
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()

# Perform some basic operations

# Select only the "name" column
df.select("name").show()

# Filter rows where age is greater than 25
df.filter(df.age > 25).show()

# Group by "city" and count the number of entries per city
df.groupBy("city").count().show()

# Stop the Spark session
spark.stop()
