import os
import time
import requests
import csv
from dotenv import load_dotenv

class TMDBClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"

    def _get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        if params is None:
            params = {}
        params["api_key"] = self.api_key
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_movie_genres(self):
        """Fetch all available movie genres from TMDB"""
        print("Fetching movie genres...")
        data = self._get("genre/movie/list")
        return data.get("genres", [])

    def get_movies_by_genre(self, genre_id, page=1):
        """Fetch popular movies by genre ID"""
        print(f"Fetching movies for genre ID: {genre_id}")
        data = self._get("discover/movie", {
            "with_genres": genre_id,
            "sort_by": "popularity.desc",
            "page": page
        })
        return data.get("results", [])

    def get_reviews(self, movie_id, limit=50):
        print(f"Fetching reviews for movie ID: {movie_id}")
        data = self._get(f"movie/{movie_id}/reviews", {"language": "en-US", "page": 1})
        results = data.get("results", [])
        return [r["content"] for r in results][:limit]

    def get_details(self, movie_id):
        print(f"Fetching details for movie ID: {movie_id}")
        return self._get(f"movie/{movie_id}")

    def get_credits(self, movie_id):
        print(f"Fetching credits for movie ID: {movie_id}")
        return self._get(f"movie/{movie_id}/credits")


