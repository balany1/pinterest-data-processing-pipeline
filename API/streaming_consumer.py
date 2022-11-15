from kafka import KafkaConsumer
import json

streaming_consumer = KafkaConsumer("Pinterest_Data_Collection",
    bootstrap_servers='localhost:9092', 
    value_deserializer= lambda x:json.loads(x.decode("utf-8")))

for msg in streaming_consumer:
    print(msg)