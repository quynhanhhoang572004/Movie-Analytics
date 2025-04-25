import os
import csv
from dotenv import load_dotenv

from client import TMDBClient

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

    def save_movies_by_genre(self, genre_id, genre_name, movie_count=20):
        """Save popular movies for a specific genre"""
        movies = self.client.get_movies_by_genre(genre_id)
        if not movies:
            print(f"No movies found for genre {genre_name}")
            return

        # Limit to the requested number of movies
        movies = movies[:movie_count]
        
        output_path = os.path.join(self.output_dir, f"movies_{genre_name.lower()}.csv")
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["title", "release_date", "popularity", "vote_average", "vote_count", "overview"])
            
            for movie in movies:
                writer.writerow([
                    movie.get("title"),
                    movie.get("release_date"),
                    movie.get("popularity"),
                    movie.get("vote_average"),
                    movie.get("vote_count", 0),
                    movie.get("overview", "").replace("\n", " ").strip()
                ])
        
        print(f"Saved {len(movies)} {genre_name} movies to {output_path}")

    def save_sample_reviews_by_genre(self, genre_id, genre_name, reviews_per_movie=5):
        """Save sample reviews for movies in a specific genre"""
        movies = self.client.get_movies_by_genre(genre_id)
        if not movies:
            print(f"No movies found for genre {genre_name}")
            return

        output_path = os.path.join(self.output_dir, f"reviews_{genre_name.lower()}.csv")
        reviews_collected = 0
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["genre", "movie_title", "review_text"])
            
            for movie in movies:
                reviews = self.client.get_reviews(movie["id"], limit=reviews_per_movie)
                for review in reviews:
                    writer.writerow([genre_name, movie["title"], review])
                    reviews_collected += 1
                
                if reviews_collected >= 100:  # Stop after collecting 50 reviews total
                    break
        
        print(f"Saved {reviews_collected} reviews for {genre_name} movies to {output_path}")

    
    def save_movie_ratings(self, genre_id, genre_name, movie_count=20):
        """Save detailed rating information for mvoies in a genre"""
        movies = self.client.get_movies_by_genre(genre_id)[:movie_count]
        if not movies:
            print(f"No movies found for genre {genre_name}")
            return
        
        output_path = os.path.join(self.output_dir, f"ratings_{genre_name.lower()}.csv")

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "title",
                "vote_average",
                "vote_count",
                "popularity",
                "rating_distribution"
            ])

            for movie in movies:
                details = self.client.get_details(movie["id"])

                rating_dist = ""
                if "rating_distribution" in details.get("externaL_ids", {}):
                    rating_dist = str(details["externalids"]["rating_distribution"])
                
                writer.writerow([
                    movie.get("title"),
                    movie.get("vote_average"),
                    movie.get("vote_count"),
                    movie.get("popularity"),
                    rating_dist
                ])
        print(f"Saved rating info for {len(movies)} {genre_name} movies to {output_path}")



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
        # Save movies with basic info
        saver.save_movies_by_genre(genre["id"], genre["name"])
        
        # Save detailed rating information
        saver.save_movie_ratings(genre["id"], genre["name"])
        
        # Save reviews for movies in this genre
        saver.save_sample_reviews_by_genre(genre["id"], genre["name"])
    