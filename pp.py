import requests
from bs4 import BeautifulSoup

# Specify the URL you want to scrape
source = 'https://www.99.co/singapore/sale/property/664a-jurong-west-street-64-hdb-9HBW6XKrkARrJ2KsWoNXbQ'

# Make a GET request to fetch the raw HTML content
response = requests.get(source)
html_content = response.content

soup = BeautifulSoup(html_content, 'html.parser')

print(soup.prettify())

# Find all div elements that have a 'data-src' attribute and are part of the carousel
carousel_divs = soup.find_all('div', attrs={'data-src': True, 'class': 'ListingGallery__image'})

# Extract the 'data-src' URLs from these divs
image_urls = [div['data-src'] for div in carousel_divs]

# Example of scraping with placeholder class names
addresses = [address.get_text() for address in soup.find_all(class_='address-class')]
captions = [caption.get_text() for caption in soup.find_all(class_='caption-class')]
descriptions = [desc.get_text() for desc in soup.find_all(class_='description-class')]
dates_uploaded = [date.get_text() for date in soup.find_all(class_='date-uploaded-class')]

# GPS locations are typically not directly displayed in HTML and might be contained in data attributes or scripts.
# Extracting this may require parsing scripts or JSON within the HTML, which is specific to the site's structure.

# Printing outputs (for demonstration)
print("Images:", image_urls)
print("Addresses:", addresses)
print("Captions:", captions)
print("Descriptions:", descriptions)
print("Dates Uploaded:", dates_uploaded)
