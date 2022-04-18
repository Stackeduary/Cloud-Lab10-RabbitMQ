# This is the broker

import pika
import random
import datetime
import json
import time

broker_host = "172.17.67.75"
broker_port = 5672

username = "admin"
password = "wabbit1"
credentials = pika.PlainCredentials(username, password)

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=broker_host,
    port=broker_port, 
    credentials=credentials))

# creates a new channel through which we can 
# send data to and listen to data from the RabbitMQ broker 
channel = connection.channel() 

while True:
    message = {
        "device_name": "SENDEWICZ_sensor",
        "temperature": random.randint(20, 40),
        "time": str(datetime.datetime.now())
    }

    message_str = json.dumps(message)

    routing_key = "iotdevice.SENDEWICZ.tempsensor"
    exchange = "SENDEWICZ"

    channel.basic_publish(
        exchange = exchange,
        routing_key = routing_key,
        body = message_str
    )
    print(f" [x] Sent {message_str}")
    time.sleep(5)

channel.close()
connection.close()