from pyspark.sql.functions import *
from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '6V6NA7NHO3WHFYGY',
        'sasl.password': 'cflt6Mn4i2UZ34oSZi/a6BK8/96Ci4np1Dq3bAW0hKgbXm0ITcd5JDTKzUGsUGvw',
        'client.id': 'ccloud-python-client-5a516a73-4aaa-4d82-8fa5-ffa0e18307a6'}
producer = Producer(conf)

# get these details from confluent kafka, search it on google.
confluentBootstrapServers = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
confluentApiKey = '6V6NA7NHO3WHFYGY'
confluentSecret = 'cflt6Mn4i2UZ34oSZi/a6BK8/96Ci4np1Dq3bAW0hKgbXm0ITcd5JDTKzUGsUGvw'
# we created the topic in confluent kafka.
confluentTopicName = 'topic_read'
confluentTargetTopicName = 'topic_write'

landing_zone = '/Volumes/misgaurav_databrciks_ws/default/misgaurav_v/retail_data'
orders_data = landing_zone + 'orders_data'
checkpoint_path = landing_zone + 'orders_cp_new_2'

%sql
GRANT USE CATALOG ON CATALOG misgaurav_databrciks_ws TO `gauravmishra7080@gmail.com`;
GRANT USE SCHEMA ON SCHEMA misgaurav_databrciks_ws.default TO `gauravmishra7080@gmail.com`;
GRANT CREATE TABLE ON SCHEMA misgaurav_databrciks_ws.default TO `gauravmishra7080@gmail.com`;

# ==================================================#
# reading from a Topic ===============================#

orders_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",confluentBootstrapServers) \
    .option("kafka.security.protocol","SASL_SSL") \
    .option("kafka.sasl.mechanism","PLAIN") \
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm","https") \
    .option("subscribe",confluentTopicName) \
    .option("startingOffsets", "earliest") \
   .option("failOnDataLoss", 'false') \
   .option("maxOffsetsPerTrigger", 50) \
   .load()
    # it read the topic from the very starting - startingTimestamp
    # microbatches are of same size - maxOffsetsPerTrigger



converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")
    # converted_orders_df is a complete string, don't treat it like a json format.

orders_schema = "order_id long,customer_id long,customer_fname \
        string,customer_lname string,city string,state string,pincode long,line_items \
        array<struct<order_item_id: long,order_item_product_id: \
        long,order_item_quantity: long,order_item_product_price: \
        float,order_item_subtotal: float>>"
    
parsed_orders_df = converted_orders_df.select("key", from_json("value", orders_schema).alias("value"), "topic", "partition", "offset","timestamp","timestampType")
    # with this we can get the json format to get the columns values. we imposed the json structure on converted_ordes_df.

parsed_orders_df.createOrReplaceTempView("orders")
    # key, & customer_id will remain same. we can remove it. 

filtered_orders = spark.sql("""select cast(key as string) as key, 
                                cast(value as string) as value from orders where 
                                value.city = 'Chicago'""")

    # the output of this df is a string, it's not a json, and this internall will converted to binary while writing to a topic.
    # Keeping key, & value as a string, while writing to the kafka topic, it'll internally converted to binary.

# writing to a kafka topic with PySpark
    
filtered_orders \
    .writeStream \
    .queryName("ingestionquery") \
    .format("kafka") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("kafka.bootstrap.servers",confluentBootstrapServers) \
    .option("kafka.security.protocol","SASL_SSL") \
    .option("kafka.sasl.mechanism","PLAIN") \
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm","https") \
    .option("topic",confluentTargetTopicName) \
    .start()

# ==================================================#
# Writing to a Topic ===============================#

def acked(err, msg):
        if err is not None:
            print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
        else:
            print('msg produced: %s' % (str(msg)))
            print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
            print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databrciks_ws/default/misgaurav_v/retail_data/orders_data/orders_input.json', mode= 'r' ) as files:
        for line in files:
            order = json.loads(line)
            customer_id = str(order['customer_id'])
            producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
            producer.poll(1)
            producer.flush()
# you'll see 2 msg in this topic: confluentTargetTopicName
# you'll see 2 msg in this topic: confluentTopicName


## re-run by placing a new files: order_input.json

def acked(err, msg):
        if err is not None:
            print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
        else:
            print('msg produced: %s' % (str(msg)))
            print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
            print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databrciks_ws/default/misgaurav_v/retail_data/orders_data/order_input.json', mode= 'r' ) as files:
        for line in files:
            order = json.loads(line)
            customer_id = str(order['customer_id'])
            producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
            producer.poll(1)
            producer.flush()

# again, you'll see 2 msg in this topic: confluentTargetTopicName
# again, you'll see 2 msg in this topic: confluentTopicName

## re-run by placing a new files: order_input_new.json

def acked(err, msg):
        if err is not None:
            print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
        else:
            print('msg produced: %s' % (str(msg)))
            print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
            print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databrciks_ws/default/misgaurav_v/retail_data/orders_data/order_input_new.json', mode= 'r' ) as files:
        for line in files:
            order = json.loads(line)
            customer_id = str(order['customer_id'])
            producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
            producer.poll(1)
            producer.flush()

# again, you'll see 2 msg in this topic: confluentTargetTopicName
# again, you'll see 2 msg in this topic: confluentTopicName

## re-run by placing a new files: order_new.json

def acked(err, msg):
        if err is not None:
            print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
        else:
            print('msg produced: %s' % (str(msg)))
            print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
            print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databrciks_ws/default/misgaurav_v/retail_data/orders_data/order_new.json', mode= 'r' ) as files:
        for line in files:
            order = json.loads(line)
            customer_id = str(order['customer_id'])
            producer.produce(topic = confluentTopicName, key = customer_id, value = line, callback = acked)
            producer.poll(1)
            producer.flush()

# again, you'll see 2 msg in this topic: confluentTargetTopicName
# again, you'll see 2 msg in this topic: confluentTopicName
