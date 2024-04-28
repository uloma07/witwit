import time
import requests, csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def fetch_html(url):
    """Fetch the HTML content of the provided URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def extract_links(html_content):
    """Extract all relevant file links from the Wikimedia Commons category page."""
    soup = BeautifulSoup(html_content, 'html.parser')
    gallery_links = []

    # Extract all links within the gallery section
    gallery_section = soup.find('ul', class_='gallery')
    if gallery_section:
        links = gallery_section.find_all('a')
        for link in links:
            if 'File:' in link.get('href', ''):
                full_url = 'https://commons.wikimedia.org' + link['href']
                gallery_links.append(full_url)

    return gallery_links


def fetch_links(url, depth, visited):
    if depth == 0 or url in visited:
        return []

    print(f"Fetching links from: {url}")
    visited.add(url)
    try:
        response = requests.get(url)
        response.raise_for_status()  # will throw an error for 4xx/5xx responses
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set()

        # Collect and filter category links
        for link in soup.find_all('a', href=True):
            full_link = urljoin(url, link['href'])
            if "https://commons.wikimedia.org/wiki/Category:" in full_link:
                links.add(full_link)

        # Recursively fetch links from child pages
        for link in list(links):
            links.update(fetch_links(link, depth - 1, visited))

        return links
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {str(e)}")
        return set()

def write_links_to_csv(links, filepath):
    """Write the initial list of links to a CSV file."""
    with open(filepath, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        #writer.writerow(['URL', 'Scraped'])
        for link in links:
            writer.writerow([link])

def main(filepath):
    # category = 'Home_offices_in_Canada'
    # url = f'https://commons.wikimedia.org/wiki/Category:{category}'

    # Starting URL and depth
    start_url = 'https://commons.wikimedia.org/wiki/Category:Building_interiors_by_building_function'
    depth = 3  # Change to 2 or 3 as needed
    visited_urls = set()

    # Get all pages
    pages = fetch_links(start_url, depth, visited_urls)

    for page in pages:
        html_content = fetch_html(page)
        links = extract_links(html_content)
        write_links_to_csv(set(links), filepath)
        time.sleep(1)

if __name__ == "__main__":
    main(r'C:\Users\ogechi\Documents\Work\TuringScraper\Data\raw\wikicommons_in.csv')
