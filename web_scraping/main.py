import os
import time
import random
import csv
import concurrent.futures
import logging
from typing import Optional, List
import requests
from bs4 import BeautifulSoup

from config import HEADERS, MAX_THREADS, OUTPUT_FILE, BASE_URL, POPULAR_MOVIES_URL
from models import Movie

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IMDbScraper:
    def __init__(self):
        self.headers = HEADERS

    def extract_movie_details(self, movie_link: str) -> Optional[Movie]:
        """Extract details for a single movie from its IMDb page."""
        time.sleep(random.uniform(0, 0.2))
        
        try:
            response = requests.get(movie_link, headers=self.headers)
            response.raise_for_status()
            movie_soup = BeautifulSoup(response.content, 'html.parser')
            
            if movie_soup is None:
                logger.warning(f"BeautifulSoup returned None for {movie_link}")
                return None
            
            title = date = rating = plot_text = None
            
            # Find the main content section
            page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
            if not page_section:
                logger.warning(f"Could not find main content section for {movie_link}")
                return None

            divs = page_section.find_all('div', recursive=False)
            if len(divs) <= 1:
                logger.warning(f"Expected content divs not found for {movie_link}")
                return None

            target_div = divs[1]
            
            # Extract title
            title_tag = target_div.find('h1')
            if not title_tag or not title_tag.find('span'):
                logger.warning(f"Title element not found for {movie_link}")
            else:
                title = title_tag.find('span').get_text()
            
            # Extract release date
            date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
            if not date_tag:
                logger.warning(f"Release date not found for {movie_link}")
            else:
                date = date_tag.get_text().strip()
            
            # Extract rating
            rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
            if not rating_tag:
                logger.warning(f"Rating not found for {movie_link}")
            else:
                rating = rating_tag.get_text()
            
            # Extract plot summary
            plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
            if not plot_tag:
                logger.warning(f"Plot summary not found for {movie_link}")
            else:
                plot_text = plot_tag.get_text().strip()
            
            # Save to CSV if all data was successfully extracted
            if all([title, date, rating, plot_text]):
                return Movie(title=title, date=date, rating=rating, plot=plot_text)
            
            logger.warning(f"Missing required data for {movie_link}. Found: title={bool(title)}, date={bool(date)}, rating={bool(rating)}, plot={bool(plot_text)}")
            return None
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred for {movie_link}: {str(e)}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error occurred for {movie_link}: {str(e)}")
            return None
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error occurred for {movie_link}: {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error occurred for {movie_link}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting movie details from {movie_link}: {str(e)}")
            return None
    
    def save_movies_to_csv(self, movies: List[Movie]) -> None:
        """Save extracted movies to CSV file."""
        try:
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.isfile(OUTPUT_FILE)
            
            with open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                
                # Write headers if file is new
                if not file_exists:
                    writer.writerow(['Title', 'Release Date', 'Rating', 'Plot'])
                
                # Write movie data
                for movie in movies:
                    writer.writerow(movie.to_csv_row())
                    logger.info(f"Saved: {movie.title} ({movie.date}) - Rating: {movie.rating}")
                    
        except FileNotFoundError:
            logger.error(f"Could not find or create file {OUTPUT_FILE}")
        except PermissionError:
            logger.error(f"Permission denied when trying to write to {OUTPUT_FILE}")
        except IOError as e:
            logger.error(f"IO error occurred while writing to {OUTPUT_FILE}: {str(e)}")
        except UnicodeEncodeError:
            logger.error("Failed to encode movie data to UTF-8")
        except csv.Error as e:
            logger.error(f"CSV writing error occurred: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while saving to CSV: {str(e)}")
    def extract_movies(self, soup: BeautifulSoup) -> List[Movie]:
        """Extract movie links and process them in parallel."""
        movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
        movies_table_rows = movies_table.find_all('li')
        movie_links = [f"{BASE_URL}{movie.find('a')['href']}" for movie in movies_table_rows]
        
        movies = []
        threads = min(MAX_THREADS, len(movie_links))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = executor.map(self.extract_movie_details, movie_links)
            movies = [movie for movie in results if movie is not None]
        
        return movies

    def run(self):
        """Main method to scrape IMDb's Most Popular Movies."""
        start_time = time.time()
        
        try:
            response = requests.get(POPULAR_MOVIES_URL, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            movies = self.extract_movies(soup)
            self.save_movies_to_csv(movies)
            
            end_time = time.time()
            logger.info(f'Total time taken: {end_time - start_time:.2f} seconds')
            
        except Exception as e:
            logger.error(f"Error in main scraping process: {str(e)}")

if __name__ == '__main__':
    scraper = IMDbScraper()
    scraper.run()