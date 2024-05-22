import yaml, flickrapi
from multiprocessing import Pool
from pymongo import MongoClient

def load_config():
    with open('../conf.yml', 'r') as configfile:
        return yaml.safe_load(configfile)

def get_indoor_collection(config):
    client = MongoClient(config['mongo']['connection'])
    db = client[config['mongo']['processed']]
    return db['indoor']

def process_record(args):
    data, config = args
    indoor_collection = get_indoor_collection(config)
    FLICKR_API_KEY = config['flickr']['FLICKR_API_KEY']
    FLICKR_API_SECRET = config['flickr']['FLICKR_API_SECRET']

    # Initialize the Flickr API client
    flickr = flickrapi.FlickrAPI(FLICKR_API_KEY, FLICKR_API_SECRET, format='parsed-json')

    try:
        url = data['url']
        print('Processing', url)
        # Extract photo ID from the image URL
        url_split = url.split('/')
        photo_data = url_split[-1]
        photo_id = photo_data.split('_')[0]

        update = {}
        # Get the full details of the photo
        #yfcc ones
        sizes = flickr.photos.getSizes(photo_id=int(photo_id))['sizes']['size']
        url_info = sizes[-1]
        update['url'] = url_info['source']
        update['width'] = url_info['width']
        update['height'] = url_info['height']
        update['media'] = url_info['media']
        indoor_collection.update_one({"_id": data["_id"]}, {'$set': update, '$unset': {'isindoor': '', 'photoid': ''}})

    except Exception as e:
        print(f"Error processing record {data['url']}: {e}")


def get_records_batch(source_collection, limitval):
    return source_collection.find({"isindoor": {"$exists": True}, "source": 'yfcc100m'}).limit(limitval)

# Adjust how you call process_record in process_urls_in_batches
def process_urls_in_batches(source_collection, config):
    batch_size = 10000  # Adjust as needed
    while True:
        records = get_records_batch(source_collection, batch_size)
        if not records:
            break
        with Pool(6) as pool:
            pool.map(process_record, [(record, config) for record in records])
    return


if __name__ == "__main__":
    config = load_config()
    indoor_collection = get_indoor_collection(config)
    process_urls_in_batches(indoor_collection, config)