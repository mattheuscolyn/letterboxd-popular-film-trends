import requests
import csv
import time
import json
import re
import os
from datetime import date
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo  # Only in Python 3.9+


BASE_URL = "https://letterboxd.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
}
MAX_FILMS = 1000


def get_film_links_from_html(html_content):
    """Extract film links and IDs from the given HTML."""
    soup = BeautifulSoup(html_content, "html.parser")
    film_items = soup.select("li.poster-container")

    films = []
    for item in film_items:
        film_div = item.select_one("div.really-lazy-load")
        if film_div:
            film_id = film_div.get("data-film-id")
            film_slug = film_div.get("data-target-link")
            if film_id and film_slug:
                full_url = BASE_URL + film_slug
                films.append((film_id, full_url))
    return films


def scrape_ajax_pages(base_genre_url, pages):
    """Scrape AJAX-loaded film pages from a genre/rating URL."""
    all_films = []
    for page in range(1, pages + 1):
        print(f'Scraping page {page}')
        ajax_url = f"{base_genre_url}page/{page}/"
        response = requests.get(ajax_url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break
        films_on_page = get_film_links_from_html(response.text)
        if not films_on_page:
            print(f"No films found on page {page}.")
            break
        all_films.extend(films_on_page)
        if len(all_films) >= MAX_FILMS:
            break
        time.sleep(2)
    return all_films[:MAX_FILMS]


def get_film_details(film_url):
    """Scrape a film page for details, including film title."""
    response = requests.get(film_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch {film_url}: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract film title
    title_tag = soup.select_one("h1.headline-1.primaryname > span.name.js-widont.prettify")
    film_title = title_tag.text.strip() if title_tag else None

    # Extract ratings
    script_tag = soup.select_one("script[type='application/ld+json']")
    rating_count, rating_value, poster_url = None, None, None
    if script_tag and script_tag.string:
        try:
            json_text = script_tag.string.strip()
            json_text = json_text.replace("/* <![CDATA[ */", "").replace("/* ]]> */", "")
            json_data = json.loads(json_text)
            rating_data = json_data.get("aggregateRating", {})
            rating_count = rating_data.get("ratingCount")
            rating_value = rating_data.get("ratingValue")
            poster_url = json_data.get("image")
        except json.JSONDecodeError:
            pass

    # Genres
    genre_section = soup.select_one("div#tab-genres .text-sluglist p")
    genres = [a.text for a in genre_section.find_all("a")] if genre_section else []

    # Runtime and TMDB type
    runtime, tmdb_type = None, None
    runtime_section = soup.select_one("p.text-link.text-footer")
    if runtime_section:
        match = re.search(r"(\d+)\s*mins", runtime_section.text)
        if match:
            runtime = match.group(1)
        tmdb_link = runtime_section.find("a", href=re.compile(r"https://www.themoviedb.org/"))
        if tmdb_link:
            match = re.search(r"themoviedb\.org/([^/]+)/\d+", tmdb_link["href"])
            if match:
                tmdb_type = match.group(1)

    # Has description
    has_description = bool(
        soup.find('meta', attrs={'name': 'description'}) or
        soup.find('meta', attrs={'property': 'og:description'})
    )

    return film_title, rating_count, rating_value, genres, runtime, tmdb_type, has_description, poster_url


def save_to_csv(data, filename):
    """Append data to the main CSV file."""
    new_df = pd.DataFrame(data, columns=[
        "Order", "Film ID", "Film URL", "Film Title", "Rating Count", "Rating Value",
        "Genres", "Runtime", "TMDB Type", "Has Description", "Poster URL", "Snapshot Date"
    ])

    if os.path.exists(filename):
        existing_df = pd.read_csv(filename)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


def main(genre_url, pages=14):
    seattle_time = datetime.now(ZoneInfo("America/Los_Angeles"))
    snapshot_date = seattle_time.date().isoformat()
    films = scrape_ajax_pages(genre_url, pages)

    if not films:
        print("No films found. Exiting.")
        return

    scraped_data = []
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(get_film_details, film_url): (i + 1, film_id, film_url)
                       for i, (film_id, film_url) in enumerate(films)}

            for future in tqdm(futures, desc="Scraping films", unit="film"):
                order, film_id, film_url = futures[future]
                try:
                    details = future.result()
                    if details is None:
                        continue
                    film_title, rating_count, rating_value, genres, runtime, tmdb_type, has_description, poster_url = details
                    scraped_data.append((
                        order, film_id, film_url, film_title, rating_count, rating_value,
                        ", ".join(genres), runtime, tmdb_type, has_description,
                        poster_url, snapshot_date
                    ))
                except Exception as e:
                    print(f"Error scraping {film_url}: {e}")
                    continue

    except KeyboardInterrupt:
        print("\nInterrupted. Saving data scraped so far...")

    save_to_csv(scraped_data, "letterboxd_popular_history.csv")



if __name__ == "__main__":
    genre_url = "https://letterboxd.com/films/ajax/popular/this/week/"
    main(genre_url, pages=14)
