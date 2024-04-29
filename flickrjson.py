from pymongo import MongoClient
import yaml
import json
import re

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)


# Define a function to search for images based on a location
def images_from_json(jsonfile, word):
    with open(jsonfile, 'r') as file:
        json_string = file.read()

        # A simplistic regex pattern to find strings that might be the end of "description" fields
        # WARNING: This could unintentionally match other parts of your JSON and is not a robust solution
        # This pattern assumes the description field ends with "}\n" (which might not always be the case)
        pattern = re.compile(r'("\}_content": ".*?)(?=",\n)', re.DOTALL)

        # Add a comma at the end of each matched string in the description content
        modified_json_string = re.sub(pattern, r'\1,', json_string)

        # Now, try to convert the modified string to a Python dictionary
        try:
            data = json.loads(modified_json_string)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        else:
            print("JSON loaded successfully.")

    # Extract photo information
    photo_list = data['photos']['photo']
    image_data = []

    # Iterate through the photos and retrieve image data
    for photo_info in photo_list:
        if int(photo_info['context']):
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


if __name__ == '__main__':
    collection = get_collection()

    try:
        images = images_from_json('./res.json','room for rent')
        print(len(images))
        if images:
            for image in images:
                try:
                    collection.insert_one(image)
                except:
                    print('small ex')
                    continue
    except Exception as e:
        print('big ex')
        print(e)




