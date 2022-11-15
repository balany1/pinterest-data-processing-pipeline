from kafka import KafkaConsumer
import json

batch_consumer = KafkaConsumer("Pinterest_Data_Collection",
    bootstrap_servers='localhost:9092', 
    value_deserializer= lambda x:json.loads(x.decode("utf-8")))

for msg in batch_consumer:
    print(msg)