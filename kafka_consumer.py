from kafka import KafkaConsumer
import json

TOPIC = "tweets_loader"
consumer = {}

#  connecting to kafka
print("connecting to kafka")
try:
    consumer = KafkaConsumer(TOPIC, bootstrap_servers='localhost:9092')
except Exception:
    print("could not connect")

else:
    print("connected")

# loading kafka message
for msg in consumer:
    tweets = msg.value.decode("utf-8")
    with open("data.txt", "a") as f:
        f.write(f"{tweets}\n")
