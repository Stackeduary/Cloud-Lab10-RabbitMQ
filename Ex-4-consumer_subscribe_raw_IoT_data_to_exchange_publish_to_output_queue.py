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

exchange = "SENDEWICZ"
queue_name = "tag_queue"
# output_queue_name = "tag_queue"

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=broker_host,
    port=broker_port, 
    credentials=credentials))

# creates a new channel through which we can 
# send data to and listen to data from the RabbitMQ broker 
channel = connection.channel() 

# durable=True means that data is queued even if there is currently no listener
# durable=True means that the queue will survive a broker restart (Copilot suggestion)

# indoor
channel.queue_declare(queue="indoor_queue", durable=True)
channel.queue_bind(exchange=exchange, queue="indoor_queue", routing_key="indoor.#")
# outdoor
channel.queue_declare(queue="outdoor_queue", durable=True)
channel.queue_bind(exchange=exchange, queue="outdoor_queue", routing_key="outdoor.#")

indoor = ['fipy_e1', 'fipy_b1', 'fipy_b2', 'fipy_b3']
outdoor = ['puhatu_b1', 'puhatu_b2', 'puhatu_b3', 'puhatu_c1', 'puhatu_c2', 'puhatu_c3', 'puhatu_l1']

def lab_callback(ch, method, properties, body):
    output_routing_key = f"tag.{method.routing_key}"
    message = json.loads(body.decode())
    if message['dev_location'] in indoor:
        output_routing_key = f"indoor.{method.routing_key}"
    elif message['dev_location'] in outdoor:
        output_routing_key = f"outdoor.{method.routing_key}"
    parsed_message_string = json.dumps(message)
    ch.basic_publish(exchange=exchange, routing_key=output_routing_key, body=parsed_message_string)

channel.basic_consume(queue=queue_name, on_message_callback=lab_callback)
channel.start_consuming()