import os
import csv
from dotenv import load_dotenv

from client import TMDBClient

MOVIE_HEADERS = [
    "id", "title", "original_title", "original_language", "release_date",
    "popularity", "vote_average", "vote_count", "adult", "video",
    "overview", "budget", "revenue", "runtime", "status", "homepage",
    "genres", "production_companies", "production_countries"
]

class MovieDataSaver:
    def __init__(self, client: TMDBClient):
        self.client = client
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.output_dir = os.path.join(script_dir, "data")
        os.makedirs(self.output_dir, exist_ok=True)

    def save_genres_list(self):
        """Save the list of all available genres"""
        genres = self.client.get_movie_genres()
        output_path = os.path.join(self.output_dir, "genres.csv")
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["genre_id", "genre_name"])
            for genre in genres:
                writer.writerow([genre["id"], genre["name"]])
        
        print(f"Saved {len(genres)} genres to {output_path}")
        return genres
    
    # Sua max_page thi no se fetch movie theo so luong
    def save_movies_by_genre(self, genre_id, genre_name, max_pages=6):
        movies = self._get_movies_by_genre(genre_id=genre_id, max_pages=max_pages)
        if not movies:
            raise ValueError(f"No movies found for genre: {genre_name}")
        
        output_path = os.path.join(self.output_dir, f"movies_{genre_name.lower()}.csv")
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(MOVIE_HEADERS)

            for movie in movies:
                details = self.client.get_details(movie["id"])
                writer.writerow(self._extract_movie(details))

        print(f"Saved {len(movies)} movies for genre {genre_name} to {output_path}")

    
    def _get_movies_by_genre(self, genre_id, max_pages=3):
        all_movies = []
        for page in range(1, max_pages + 1):
            result = self.client.get_movies_by_genre(genre_id=genre_id, page=page)
            if not result:
                break
            all_movies.extend(result)
        return all_movies

    @staticmethod
    def _extract_movie(details: dict) -> list:
        return [
            details.get("id"),
            details.get("title"),
            details.get("original_title"),
            details.get("original_language"),
            details.get("release_date"),
            details.get("popularity"),
            details.get("vote_average"),
            details.get("vote_count"),
            details.get("adult"),
            details.get("video"),
            details.get("overview", "").replace("\n", " ").strip(),
            details.get("budget"),
            details.get("revenue"),
            details.get("runtime"),
            details.get("status"),
            details.get("homepage"),
            "; ".join([g["name"] for g in details.get("genres", [])]),
            "; ".join([c["name"] for c in details.get("production_companies", [])]),
            "; ".join([c["name"] for c in details.get("production_countries", [])]),
        ]



if __name__ == "__main__":
    # Load .env from the parent directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv("TMDB_API_KEY")

    client = TMDBClient(api_key)
    saver = MovieDataSaver(client)

    # First, get and save all available genres
    genres = saver.save_genres_list()
    
    # Then work with specific genres (here using the first 3 as an example)
    for genre in genres[:5]:
        saver.save_movies_by_genre(genre["id"], genre["name"])