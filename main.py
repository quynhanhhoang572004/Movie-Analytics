import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from scraper.client import TMDBClient
from scraper.fetcher import TMDBFetcher
from scraper.uploader import GCSUploader

MAX_WORKERS = 5
GCS_PATH_PREFIX = "data/movies/"

if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY")
    bucket_name = os.getenv("GCP_BUCKET_NAME")
    key_path = "keys/my_key.json"

    client = TMDBClient(api_key)
    fetcher = TMDBFetcher(client)
    uploader = GCSUploader(bucket_name, key_path)

    genres = fetcher.get_genres()

    def process_genre(genre):
        genre_id = genre["id"]
        genre_name = genre["name"]
        safe_name = genre_name.lower().replace(" ", "_")
        blob_name = f"{GCS_PATH_PREFIX}movies_{safe_name}.parquet"

        print(f"Fetching movies for genre: {genre_name}")
        movies = fetcher.get_movies_by_genre(genre_id, max_pages=6)
        if not movies:
            print(f"No movies found for genre: {genre_name}")
            return False
        return uploader.upload_parquet(movies, blob_name)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_genre, genre): genre["name"] for genre in genres[:10]}

        for future in as_completed(futures):
            genre_name = futures[future]
            try:
                future.result()
                print(f"Done with genre: {genre_name}")
            except Exception as e:
                print(f"Error with genre {genre_name}: {e}")