import time

class TMDBFetcher:
    def __init__(self, client):
        self.client = client

    def get_genres(self):
        return self.client.get_movie_genres()

    def get_movies_by_genre(self, genre_id, max_pages=6):
        all_movies = []
        for page in range(1, max_pages + 1):
            try:
                result = self.client.get_movies_by_genre(genre_id=genre_id, page=page)
                if not result:
                    break
                all_movies.extend(result)
                time.sleep(0.25) 
            except Exception as e:
                print(f"error fetching page {page} for genre {genre_id}: {e}")
                break
        return all_movies