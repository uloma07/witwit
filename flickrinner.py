from pymongo import MongoClient
import flickrapi
import yaml
import time
import os


with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

FLICKR_API_KEY = config['flickr']['FLICKR_API_KEY']
FLICKR_API_SECRET = config['flickr']['FLICKR_API_SECRET']

# Initialize the Flickr API client
flickr = flickrapi.FlickrAPI(FLICKR_API_KEY, FLICKR_API_SECRET, format='parsed-json')

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['flickr']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection


# Function to fetch details for each image URL
def fetch_image_details(owner_name, page):
    try:
        print(owner_name)
        # Search for photos in the specified location
        photos = flickr.photos.search(
            user_id=owner_name,
            media='photos',  # Only retrieve photos
            has_geo=1,  # Filter photos with geographic location data
            per_page=500,
            page=page,
            license='9,10',
            extras='description, license, date_upload, date_taken, owner_name, icon_server, original_format, last_update, geo, tags, machine_tags, o_dims, views, media, path_alias, url_l, url_o',
        )

        # Extract photo information
        photo_list = photos['photos']['photo']
        image_data = []

        # Iterate through the photos and retrieve image data
        for photo_info in photo_list:
                # Extract additional information
                title = photo_info.get('title')
                latitude = photo_info.get('latitude')
                longitude = photo_info.get('longitude')
                description = photo_info['description']['_content']
                img_license = photo_info['license']
                date_upload = photo_info.get('dateupload')
                date_taken = photo_info.get('datetaken')
                owner_name = photo_info['owner']
                original_format = photo_info.get('originalformat')
                last_update = photo_info.get('lastupdate')
                tags = photo_info['tags']
                machine_tags = photo_info.get('machine_tags')
                media = photo_info['media']
                url_l = photo_info.get('url_l')
                width_l = photo_info.get('width_l')
                height_l = photo_info.get('height_l')
                url = photo_info.get('url_o', url_l)
                width = photo_info.get('width_o', width_l)
                height = photo_info.get('height_o', height_l)

                # Append image data to the list
                im = {
                    'title': title,
                    'description': description,
                    'license': img_license,
                    'date_upload': date_upload,
                    'date_taken': date_taken,
                    'owner_name': owner_name,
                    'original_format': original_format,
                    'last_update': last_update,
                    'latitude': latitude,
                    'longitude': longitude,
                    'tags': tags,
                    'machine_tags': machine_tags,
                    'media': media,
                    'url': url,
                    'width': width,
                    'height': height
                }
                image_data.append(im)

        return image_data
    except Exception as e:
        print('ERR:', e)
        return {}

def get_in_docs():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    indoor_collection = client[config['mongo']['processed']]['indoor']
    in_data = indoor_collection.find({"owner_name": {"$exists": True}})
    return in_data

collection = get_collection()
for record in get_in_docs():
    page = 1
    while page:
        try:
            images = fetch_image_details(record['owner_name'], page=page)
            if images:
                for image in images:
                    try:
                        collection.insert_one(image)
                    except Exception as e:
                        continue
                page += 1
            else:
                page = 0
            # Pause to avoid overloading the API
            time.sleep(0.1)
        except Exception as e:
            page = 0
            # print(e)

