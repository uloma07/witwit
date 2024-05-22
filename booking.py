import requests
from bs4 import BeautifulSoup
import time, yaml
from pymongo import MongoClient
import pandas as pd
with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['booking']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection


collection = get_collection()

# Load your dataset
df = pd.read_csv(r'C:\Users\ogechi\Downloads\data\locations.csv')
imgs = pd.read_csv(r'C:\Users\ogechi\Downloads\data\images.csv')
# For case-insensitive search
locations = df[df.apply(lambda row: row.astype(str).str.contains('singapore', case=False).any(), axis=1)]

data = pd.merge(locations, imgs, left_on='id', right_on='location_id', how='inner')
for i, row in data.iterrows():
    time.sleep(1)
    try:
        # Sending a HTTP request to the specified URL
        response = requests.get(row['url'])

        # Parsing the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Assuming the hotel description is contained within a meta tag with name='description'
        hotel_description_box = soup.find(id="property_description_content")

        hotel_description = None

        # Extracting the content attribute from the meta tag
        if hotel_description_box and hotel_description_box.find('p'):
            hotel_description = hotel_description_box.find('p').string

        image = {
            'page_source': row['url'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'address': row['address'],
            'type': row['type'],
            'source': 'booking.com',
            'description': hotel_description if hotel_description else '',
            'url': row['image_url']
        }
        collection.insert_one(image)
    except Exception as e:
        print(e)
        continue



