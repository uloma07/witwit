from pymongo import MongoClient
import flickrapi
import time
import yaml
import datetime

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

FLICKR_API_KEY = config['flickr']['FLICKR_API_KEY']
FLICKR_API_SECRET = config['flickr']['FLICKR_API_SECRET']
words_file = config['llmwords']

# Initialize the Flickr API client
flickr = flickrapi.FlickrAPI(FLICKR_API_KEY, FLICKR_API_SECRET, format='parsed-json')

# Define a function to search for images based on a location
def search_images_by_word(word, per_page=10, page=1):
    # Search for photos in the specified location
    photos = flickr.photos.search(
        #accuracy=3,
        min_taken_date=datetime.datetime(1900, 1, 1),
        #min_upload_date=datetime.datetime(1950, 1, 1),
        text=word.replace('_', ' '),
        media='photos',  # Only retrieve photos
        has_geo=1,  # Filter photos with geographic location data
        #geo_context=1,
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
                'keyword': word
            }
            image_data.append(im)

    return image_data


def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['flickr']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection

def get_keywords():
    # Read words from words file
    with open(words_file, 'r', newline='', encoding='utf-8') as wordfile:
        words = [line.split('/')[-1].split()[0] for line in wordfile]

    return words

if __name__ == '__main__':
    collection = get_collection()
    keywords = get_keywords()
    for word in keywords:
        print(word)
        page = 1
        while page:
            try:
                images = search_images_by_word(word, per_page=500, page=page)
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
                time.sleep(0.5)
            except Exception as e:
                page = 0
                # print(e)




