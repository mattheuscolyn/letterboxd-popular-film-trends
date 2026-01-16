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
    film_items = soup.select("li.posteritem")

    films = []
    for item in film_items:
        film_div = item.select_one("div.react-component")
        if film_div:
            film_id = film_div.get("data-film-id")
            film_slug = film_div.get("data-target-link")
            if film_id and film_slug:
                full_url = BASE_URL + film_slug
                films.append((film_id, full_url))
    return films


def scrape_ajax_pages_single_pass(base_genre_url, pages):
    """Scrape all pages in a single pass, returning films with their page numbers.
    Returns a dict mapping film_id -> (film_url, earliest_page_number, position_in_page)."""
    film_data = {}  # film_id -> (film_url, min_page, position)
    
    for page in range(1, pages + 1):
        print(f'  Scraping page {page}')
        ajax_url = f"{base_genre_url}page/{page}/"
        response = requests.get(ajax_url, headers=HEADERS)
        if response.status_code != 200:
            print(f"  Failed to fetch page {page}: {response.status_code}")
            break
        films_on_page = get_film_links_from_html(response.text)
        if not films_on_page:
            print(f"  No films found on page {page}.")
            break
        
        # Track each film with its position on this page
        for position, (film_id, film_url) in enumerate(films_on_page, start=1):
            if film_id not in film_data:
                # First time seeing this film - record it
                film_data[film_id] = (film_url, page, position)
            else:
                # Film seen before - keep the earliest page it appeared on
                existing_url, existing_page, existing_pos = film_data[film_id]
                if page < existing_page:
                    film_data[film_id] = (film_url, page, position)
        
        time.sleep(2)
    
    return film_data


def scrape_top_films(base_genre_url, pages=14, num_passes=2):
    """Scrape pages multiple times and compare to identify the top 1000 films.
    Films that appear consistently in early pages across passes are prioritized.
    Returns list of (film_id, film_url) tuples ordered by their position."""
    print(f"Starting {num_passes} passes to identify top {MAX_FILMS} films...")
    
    all_passes_data = []
    
    for pass_num in range(1, num_passes + 1):
        print(f"\nPass {pass_num}/{num_passes}:")
        pass_data = scrape_ajax_pages_single_pass(base_genre_url, pages)
        all_passes_data.append(pass_data)
        print(f"  Pass {pass_num} complete: found {len(pass_data)} unique films")
        
        if pass_num < num_passes:
            # Wait a bit between passes to allow rankings to stabilize
            print("  Waiting 5 seconds before next pass...")
            time.sleep(5)
    
    # Combine data from all passes
    # For each film, track: earliest page seen, latest page seen, number of passes it appeared in
    film_scores = {}  # film_id -> (earliest_page, latest_page, num_passes, film_url)
    
    for pass_data in all_passes_data:
        for film_id, (film_url, page, position) in pass_data.items():
            if film_id not in film_scores:
                film_scores[film_id] = {
                    'earliest_page': page,
                    'latest_page': page,
                    'num_passes': 1,
                    'film_url': film_url,
                    'total_positions': [page]  # track all pages it appeared on
                }
            else:
                score = film_scores[film_id]
                score['earliest_page'] = min(score['earliest_page'], page)
                score['latest_page'] = max(score['latest_page'], page)
                score['num_passes'] += 1
                score['total_positions'].append(page)
    
    # Calculate average page position for each film
    for film_id, score in film_scores.items():
        score['avg_page'] = sum(score['total_positions']) / len(score['total_positions'])
    
    # Sort films by:
    # 1. Earliest page they appeared on (primary)
    # 2. Average page position (secondary)
    # 3. Number of passes they appeared in (tertiary - prefer films seen in all passes)
    
    sorted_films = sorted(
        film_scores.items(),
        key=lambda x: (
            x[1]['earliest_page'],
            x[1]['avg_page'],
            -x[1]['num_passes']  # negative because more passes = better
        )
    )
    
    # Take top MAX_FILMS
    top_films = sorted_films[:MAX_FILMS]
    
    print(f"\nIdentified top {len(top_films)} films:")
    print(f"  Films appearing in all {num_passes} passes: {sum(1 for _, s in top_films if s['num_passes'] == num_passes)}")
    print(f"  Films appearing in {num_passes-1} passes: {sum(1 for _, s in top_films if s['num_passes'] == num_passes-1)}")
    
    # Return as list of (film_id, film_url) tuples
    return [(film_id, score['film_url']) for film_id, score in top_films]


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
    """Append data to the main CSV file, ensuring no duplicate Film IDs per snapshot date."""
    new_df = pd.DataFrame(data, columns=[
        "Order", "Film ID", "Film URL", "Film Title", "Rating Count", "Rating Value",
        "Genres", "Runtime", "TMDB Type", "Has Description", "Poster URL", "Snapshot Date"
    ])
    
    # Deduplicate within the new data by keeping first occurrence of each Film ID per snapshot date
    new_df = new_df.drop_duplicates(subset=['Film ID', 'Snapshot Date'], keep='first')
    print(f"New data: {len(data)} rows -> {len(new_df)} unique rows after deduplication")

    if os.path.exists(filename):
        existing_df = pd.read_csv(filename)
        # Also deduplicate existing data to clean up any historical duplicates
        existing_df = existing_df.drop_duplicates(subset=['Film ID', 'Snapshot Date'], keep='first')
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df.to_csv(filename, index=False)
    
    # Report on uniqueness for the latest snapshot
    if len(new_df) > 0:
        snapshot_date = new_df['Snapshot Date'].iloc[0]
        snapshot_df = combined_df[combined_df['Snapshot Date'] == snapshot_date]
        unique_count = snapshot_df['Film ID'].nunique()
        print(f"Data saved to {filename}")
        print(f"Snapshot {snapshot_date}: {len(snapshot_df)} total rows, {unique_count} unique Film IDs")


def main(genre_url, pages=14, num_passes=2):
    seattle_time = datetime.now(ZoneInfo("America/Los_Angeles"))
    snapshot_date = seattle_time.date().isoformat()
    films = scrape_top_films(genre_url, pages, num_passes)

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
    main(genre_url, pages=14, num_passes=2)
