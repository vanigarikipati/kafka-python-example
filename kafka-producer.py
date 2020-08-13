import json

from kafka import KafkaProducer

BootStrap_Server = "localhost:9092"
topic_name = "testTopic"
# message = 'Hello, World testing'  # Here Message should be Byte Array if data is NOT Serialized
message = {"category_id" : 1, "category_department_id" : 2, "category_name" : "Football"}
# Create a Kafka Producer Object W/O Serialization
# producer = KafkaProducer(bootstrap_servers=BootStrap_Server)

# Create a Kafka Producer Object W/ Serialization
producer = KafkaProducer(bootstrap_servers=BootStrap_Server,
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))
print("Sending Data from Kafka Producer")

# Sending data to Consumer
producer.send(topic_name, message).get()

