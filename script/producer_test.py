
from kafka_producer.producer import Producer

producer = Producer(config_path="config.yaml")
producer.stream_data(genre_limit=2)