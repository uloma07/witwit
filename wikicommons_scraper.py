import requests, csv, yaml, time
from bs4 import BeautifulSoup
from pymongo import MongoClient

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['wikicommons']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection

# Load HTML content from a URL
def load_html(url):
    print(url)
    response = requests.get(url)
    response.raise_for_status()  # Raises an error for bad responses
    return response.text

# Extract information from the HTML
def extract_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Image URL
    image_url = soup.find('div', {'class': 'fullImageLink'}).a['href']

    # Extract image resolution
    file_history_table = soup.find('table', {'class': 'filehistory'})

    # Date
    date_element = file_history_table.select_one('td.filehistory-selected > a')
    date = date_element.text

    img_tag = soup.find('img')

    file_height = img_tag.get('data-file-height')
    file_width = img_tag.get('data-file-width')

    # GPS coordinates
    camera_location = soup.find('span', {'class': 'geo'})
    if camera_location:
        camera_location = camera_location.text.strip().split(';')
        latitude = float(camera_location[0].strip())
        longitude = float(camera_location[1].strip())
    else:
        return {}

    # Description
    description = soup.find('td', {'class': 'description'}).div
    description = description.text.strip() if description else ""

    # Author
    author = soup.find('a', {'class': 'mw-userlink'}).get('href')

    # License
    license = soup.find('span', {'class': 'licensetpl_link'})
    license = license.text.strip() if license else ""

    # Caption
    caption = soup.find('div', {'class': 'wbmi-caption-value'})
    caption = caption.text.strip() if caption else ""

    # Metadata (structured data)
    table = soup.find('table', id='mw_metadata')

    # Dictionary to hold the metadata
    metadata = {}

    # Iterate through each row in the table
    for row in table.find_all('tr'):
        header = row.find('th').get_text(strip=True) if row.find('th') else None
        value = row.find('td').get_text(strip=True) if row.find('td') else None
        if header and value:
            metadata[header] = value


    return {
        'url': image_url,
        'latitude': latitude,
        'longitude': longitude,
        'description': description,
        'owner_name': author,
        'last_update': date,
        'width': file_width,
        'height': file_height,
        'license': license,
        'caption': caption,
        'metadata': metadata
    }

def scrape_file_page(url):
    """Scrape details from each file page and pretend to return some data."""
    # Simulate data scraping
    html_content = load_html(url)
    data = extract_data(html_content)
    if data:
        data['source'] = url
    return data

def main(filepath, collection):
    """Read links from CSV, scrape them, and update their status."""
    with open(filepath, mode='r+', newline='', encoding='utf-8') as file:
        rows = list(csv.reader(file))
        for i, row in enumerate(rows):  # Skip header
            try:
                result = scrape_file_page(row[0])
                if result:
                    collection.insert_one(result)
            except Exception as e:
                print(e)
                print(result)
                continue
            time.sleep(1)


# Main function to run the extraction
if __name__ == "__main__":
    collection = get_collection()
    main(r'C:\Users\ogechi\Documents\Work\TuringScraper\Data\raw\wikicommons_in.csv', collection)
