from pyspark.sql.functions import struct, to_json, col
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Assume df has columns: A (int), B (string), C (float)
# while writing pyspark df to a kafka topic 
# make sure one columns would be key as a string format
# all columns should become values as a string format

spark = SparkSession.builder.appName('yo') \
        .config('spark.sql.shuffle.partitions', 3) \
        .config('spark.driver.bindAddress', 'localhost') \
        .config('spark.driver.port', 4050) \
        .config('spark.ui.port', 4051) \
        .master('local[*]') \
        .getOrCreate() \
        
spark.sparkContext.setLogLevel("WARN")
        
l = [(101,'Gaurav', 20), (102,'Anima', 30), (103, 'Ashish', 40), (104,'Tushar', 50), (105,'Aditya',60)]
df = spark.createDataFrame(l, ['id', 'Name', 'salary'])

df.show()
# +---+------+------+
# | id|  Name|salary|
# +---+------+------+
# |101|Gaurav|    20|
# |102| Anima|    30|
# |103|Ashish|    40|
# |104|Tushar|    50|
# |105|Aditya|    60|
# +---+------+------+

kafka_prepared_df = df.select(
    # 1. Column 'A' becomes the Kafka KEY (cast to string for partition routing)
    col("id").cast("string").alias("key"),
    
    # 2. ALL columns (A, B, C) packed into a JSON string as the Kafka VALUE
    to_json(struct("*")).alias("value"),
    struct("*").alias('value1')
)

print(kafka_prepared_df.dtypes) # [('key', 'string'), ('value', 'string'), ('value1', 'struct<id:bigint,Name:string,salary:bigint>')]

kafka_prepared_df.show(truncate = False)
# +---+--------------------------------------+-----------------+
# |key|value                                 |value1           |
# +---+--------------------------------------+-----------------+
# |101|{"id":101,"Name":"Gaurav","salary":20}|{101, Gaurav, 20}|
# |102|{"id":102,"Name":"Anima","salary":30} |{102, Anima, 30} |
# |103|{"id":103,"Name":"Ashish","salary":40}|{103, Ashish, 40}|
# |104|{"id":104,"Name":"Tushar","salary":50}|{104, Tushar, 50}|
# |105|{"id":105,"Name":"Aditya","salary":60}|{105, Aditya, 60}|
# +---+--------------------------------------+-----------------+

print(kafka_prepared_df.head(2))
# [Row(key='101', value='{"id":101,"Name":"Gaurav","salary":20}', value1=Row(id=101, Name='Gaurav', salary=20)), Row(key='102', value='{"id":102,"Name":"Anima","salary":30}', value1=Row(id=102, Name='Anima', salary=30))]

print(kafka_prepared_df.head(20)[1]) # Row(key='102', value='{"id":102,"Name":"Anima","salary":30}', value1=Row(id=102, Name='Anima', salary=30))
print(kafka_prepared_df.head(20)[1][1]) # {"id":102,"Name":"Anima","salary":30}

# Write to Kafka Topic
(
    kafka_prepared_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "my_target_topic")
    .save()
)
