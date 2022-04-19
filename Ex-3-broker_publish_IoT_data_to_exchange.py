# This is the broker

import pika
import random
import datetime
import json
import time
import pandas as pd

broker_host = "172.17.67.75"
broker_port = 5672

username = "admin"
password = "wabbit1"
credentials = pika.PlainCredentials(username, password)

exchange = "SENDEWICZ"
queue_name = "puhatu_queue"
output_routing_key = "puhatu.#"
routing_key = "puhatu.SENDEWICZ.raw"

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=broker_host,
    port=broker_port, 
    credentials=credentials))

# creates a new channel through which we can 
# send data to and listen to data from the RabbitMQ broker 
channel = connection.channel() 

# create a new queue
channel.queue_declare(queue=queue_name, durable=True)

# bind the queue to the exchange with the routing key
channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=output_routing_key)

# read a CSV using a pandas dataframe
df = pd.read_csv("https://courses.cs.ut.ee/2022/cloud/spring/uploads/Main/puhatu.csv")

df = df.sample(len(df))

messages = json.loads(df.to_json(orient="records"))

for i in range(len(messages)):
    message_str = json.dumps(messages[i])
    channel.basic_publish(
        exchange = exchange,
        routing_key = routing_key,
        body = message_str
    )
    print(f" [x] Sent {message_str}")
    print(f" Message published to the exchange {exchange} with routing key {routing_key}")
    time.sleep(2)

channel.close()
connection.close()
