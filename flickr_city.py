from pymongo import MongoClient
import flickrapi
import csv
import time
import yaml
import datetime

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

FLICKR_API_KEY = config['flickr']['FLICKR_API_KEY']
FLICKR_API_SECRET = config['flickr']['FLICKR_API_SECRET']
locations_file = config['locations']

# Initialize the Flickr API client
flickr = flickrapi.FlickrAPI(FLICKR_API_KEY, FLICKR_API_SECRET, format='parsed-json')


# def sanitize_string(input_string):
#     # Replace newline characters with a space
#     sanitized_string = input_string.replace('\n', '<newl>')
#     return sanitized_string


# Define a function to search for images based on a location
def search_images_by_location(location, per_page=500, page=1):
    # Search for photos in the specified location
    photos = flickr.photos.search(
        accuracy=3,
        min_taken_date=datetime.datetime(1900, 1, 1),
        #min_upload_date=datetime.datetime(1900, 1, 1),
        text=location['city_ascii'],
        media='photos',  # Only retrieve photos
        has_geo=1,  # Filter photos with geographic location data
        per_page=per_page,  # Number of photos per page
        page=page,
        license='1,2,3,4,5,6,7,8,9,10',
        extras='description, license, date_upload, date_taken, owner_name, original_format, last_update, geo, tags, machine_tags, o_dims, media, url_l, url_o'
    )

    # Extract photo information
    photo_list = photos['photos']['photo']
    image_data = []

    # Iterate through the photos and retrieve image data
    for photo_info in photo_list:
        #if int(photo_info['context']):
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
                'height': height,
                'location_id': location['id']
            }
            image_data.append(im)

    return image_data


def write_to_csv():
    # Search for images and save data to CSV files
    with open('Data/raw/image_data.csv', 'w', newline='', encoding='utf-8') as image_file:
        fieldnames = ['location_id', 'latitude', 'longitude', 'url', 'title', 'description', 'license',
                      'date_upload', 'date_taken', 'owner_name', 'original_format', 'last_update', 'tags',
                      'machine_tags', 'width', 'height', 'media']
        image_writer = csv.DictWriter(image_file, fieldnames=fieldnames)
        image_writer.writeheader()

        for location in locations:
            try:
                images = search_images_by_location(location, radius=15, per_page=500)
                for image in images:
                    image_writer.writerow(image)
                # Pause for a moment to avoid overloading the API
                time.sleep(1)
            except Exception as e:
                print(e)

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['flickr']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection

def get_locations():
    # Read locations from CSV file
    locations = []
    with open(locations_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            locations.append(row)
    return locations

if __name__ == '__main__':
    collection = get_collection()
    locations = get_locations()
    l = len(locations)//2
    for location in locations:
        page = 1
        while page:
            try:
                images = search_images_by_location(location, radius=19, per_page=500, page=page)
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
                page = 0
                # print(e)





