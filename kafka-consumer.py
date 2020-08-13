from json import loads

from kafka import KafkaConsumer

BootStrap_Server = "localhost:9092"
topic_name = "testTopic"

# Create Kafka Consumer Object W/O De-Serialization
# consumer = KafkaConsumer(topic_name, bootstrap_servers=BootStrap_Server)

# Create Kafka Consumer Object W/ De-Serialization
consumer = KafkaConsumer(topic_name, bootstrap_servers=BootStrap_Server,
                         value_deserializer=lambda m: loads(m.decode('ascii')))
print("Receiving Data from Producer")

# Display data received from producer
for message in consumer:
    print(message.value)
