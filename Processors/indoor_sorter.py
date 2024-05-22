import yaml
from multiprocessing import Pool
from pymongo import MongoClient
from places365.in_out_mongo import *


source_name='flickr'
def load_config():
    with open('conf.yml', 'r') as configfile:
        return yaml.safe_load(configfile)

def get_indoor_collection(config):
    client = MongoClient(config['mongo']['connection'])
    db = client[config['mongo']['processed']]
    return db['indoor']

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config[source_name]['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection


def process_record(args):
    data, labels_IO, config = args
    client = MongoClient(config['mongo']['connection'])
    indoor_collection = client[config['mongo']['processed']]['indoor']
    outdoor_collection = client[config['mongo']['processed']]['outdoor']
    collection_name = config[source_name]['collection']
    source_collection = client[config['mongo']['raw']][collection_name]

    try:
        url = data['url']
        print('Processing', url)
        model = load_model()
        model.to(device)
        if is_indoor(url, model, labels_IO):
            indoor_collection.insert_one(data)
        else:
            outdoor_collection.insert_one(data)
    except Exception as e:
        try:
            outdoor_collection.insert_one(data)
            outdoor_collection.update_one({"_id": data["_id"]}, {"$set": {"problematic": True}})
            print(f"Error processing record {data['url']}: {e}")
        except:
            print(f"Error updating record {data['url']}: {e}")

    try:
        # Update the original record with 'processed' flag
        source_collection.update_one({"_id": data["_id"]}, {"$set": {"processed": True}})
    except:
        print('url exists', data['url'])


def get_records_batch(source_collection, limitval):
    return source_collection.find({"processed": {"$exists": False}}).limit(limitval)
#    return source_collection.find({"processed": {"$exists": False}, "category": {"$exists": True}}).limit(limitval)

# Adjust how you call process_record in process_urls_in_batches
def process_urls_in_batches(source_collection, labels_IO, config):
    batch_size = 10000  # Adjust as needed
    while True:
        records = get_records_batch(source_collection, batch_size)
        if not records:
            break
        with Pool(5) as pool:
            pool.map(process_record, [(record, labels_IO, config) for record in records])



if __name__ == "__main__":
    config = load_config()
    labels_IO = load_labels()
    source_collection = get_collection()
    print(f'Processing records from...{source_name}')
    process_urls_in_batches(source_collection, labels_IO, config)
