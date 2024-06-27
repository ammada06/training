from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# create spark session with hive enabled
spark = SparkSession.builder.appName("nvidia_stock_price").enableHiveSupport().getOrCreate()
# .config("spark.jars", "/Users/ammad/Desktop/config/postgresql-42.7.2.jar") \

# postgress connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "nvidia_stock_price"

# hive database and table names
hive_database_name = "ammad"
hive_table_name = "nvidia_stock_price"

# read data from postgres table into dataframe :
postgres_df = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
postgres_df.show(3)

# read the existing_data in hive table
existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))    #table("project1db.carinsuranceclaims")
existing_hive_data.show(3)

# determine the incremental data
incremental_data_df = postgres_df.join(existing_hive_data.select("Date"), postgres_df["Date"] == existing_hive_data["Date"], "left_anti")
print('Incremental data:')
incremental_data_df.show()

# add any new rows in postgress to hive
if incremental_data_df.count() > 0:
    incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
else:
    print("No new records been inserted in PostgresSQL table.")

# stop Spark session
spark.stop()
