import os
import csv
import yaml
from typing import Optional

from io import StringIO
from kafka import KafkaProducer
from dotenv import load_dotenv
from scraper.movie import MovieDataSaver
from scraper.client import TMDBClient



class Producer:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        kafka_cfg = self.config["kafka"]

        self.topic = kafka_cfg["topic"]
        self.bootstrap_servers = kafka_cfg["bootstrap_servers"]

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: v.encode("utf-8")
        )

        load_dotenv()
        api_key = os.getenv("TMDB_API_KEY")
        self.client = TMDBClient(api_key)
        self.scraper = MovieDataSaver(self.client)

    
    def _format_as_csv(self, row: list[str]) -> str:
        buffer = StringIO()
        writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(row)
        return buffer.getvalue().strip()
    
    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    

    def stream_data(self, genre_limit: int =5):
        genres = self.scraper.save_genres_list()[:genre_limit]
        for genre in genres:
            genre_id=genre["id"]
            genre_name = genre["name"].lower()
            print("fScraping movies for genre: {genre_name}")
            for row in self.scraper.yield_movies_by_genre(genre_id, genre_name):
                csv_message = self._format_as_csv(row)
                self.producer.send(self.config.topic, csv_message)
        self.producer.flush()

        print("TMDB Data Streams into Message Queue")


    @classmethod
    def run(cls):
        producer = cls()
        producer.stream_data()
