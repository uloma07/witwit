import csv
from pymongo import MongoClient
import yaml
import time


with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['airbnb']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection


def extract_data(filename):
    data_dict = {}

    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            listing_id = row['id']
            image_url = row['picture_url']
            description = row['description']
            latitude= float(row['latitude']),
            longitude= float(row['longitude'])
            owner_name = row['host_name']
            property_type = row['property_type']
            room_type = row['room_type']
            price = row['price']
            last_update = row['last_scraped']
            name = row['name']
            neighborhood_overview = row['neighborhood_overview']


            data_dict[listing_id] = {
                'url': image_url,
                'description': description,
                'latitude': latitude,
                'longitude': longitude,
                'owner_name': owner_name
            }

    return data_dict






if __name__ == '__main__':
    collection = get_collection()
    data = WikiDataQueryResults(query, page).load()
    if data:
        for image in data:
            try:
                collection.insert_one(image)
            except Exception as e:
                continue
        #collection.insert_many(data, ordered=False)
    else:
        break
    page += 1
    time.sleep(0.5)