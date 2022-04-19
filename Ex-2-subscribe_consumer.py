# This is the consumer

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

queue_name = "SENDEWICZ_queue"
routing_key = "iotdevice.SENDEWICZ.tempsensor"
exchange = "SENDEWICZ"

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=broker_host,
    port=broker_port, 
    credentials=credentials))

# creates a new channel through which we can 
# send data to and listen to data from the RabbitMQ broker 
channel = connection.channel() 

# durable=True means that data is queued even if there is currently no listener
# durable=True means that the queue will survive a broker restart (Copilot suggestion)
channel.queue_declare(queue=queue_name, durable=True)

routing_key = "iotdevice.*.tempsensor"

channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)

def lab_callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    print(f"Inbox: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=queue_name, on_message_callback=lab_callback)
channel.start_consuming()
