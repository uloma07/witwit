from pymongo import MongoClient
import flickrapi
import pandas as pd
import yaml
import time
import datetime


with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

FLICKR_API_KEY = config['flickr']['FLICKR_API_KEY']
FLICKR_API_SECRET = config['flickr']['FLICKR_API_SECRET']

# Initialize the Flickr API client
flickr = flickrapi.FlickrAPI(FLICKR_API_KEY, FLICKR_API_SECRET, format='parsed-json')

# Read the CSV file containing the image URLs
csv_file_path = r'C:\Users\ogechi\Documents\Work\CF\temp.csv'  # Replace with your file path
df = pd.read_csv(csv_file_path, header=None, names=['Image URL', 'Latitude', 'Longitude', 'Target Latitude', 'Target Longitude'])


def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['flickr']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection


# Function to fetch details for each image URL
def fetch_image_details(url, page):

    # Extract photo ID from the image URL
    photo_data = url.split('/')[-1]
    photo_id = photo_data.split('_')[0]
    secret = photo_data.split('_')[1]

    try:
        # Get the full details of the photo
        photo_info = flickr.photos.getInfo(photo_id=int(photo_id), secret=secret)
        owner_name = photo_info['photo']['owner']['nsid']
        print(owner_name)

        # Search for photos in the specified location
        photos = flickr.photos.search(
            user_id=owner_name,
            media='photos',  # Only retrieve photos
            has_geo=1,  # Filter photos with geographic location data
            per_page=500,
            page=page,
            license="1,2,3,4,5,6,7,8",
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
        print('Err', e)
        return {}



collection = get_collection()
# Fetch details for each image URL
for _, row in df.iterrows():
    url = row['Image URL']
    if('flickr' not in url): continue

    page = 1
    while page:
        try:
            images = fetch_image_details(url, page=page)
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
            time.sleep(0.3)
        except Exception as e:
            print('Errorr', e)
            page = 0
            # print(e)

