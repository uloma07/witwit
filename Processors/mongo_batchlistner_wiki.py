from pymongo import MongoClient
import yaml
from places365.in_out_mongo import *

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)


def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['wikicommons']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    return collection

def get_in_out_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    indoor_collection = client[config['mongo']['processed']]['indoor']
    outdoor_collection = client[config['mongo']['processed']]['outdoor']
    return indoor_collection, outdoor_collection


def process_images(unprocessed_data, model, labels_IO, source_collection, indoor_collection, outdoor_collection):

    img_urls = [doc['url'] for doc in unprocessed_data]
    votes = batch_is_indoor(img_urls, model, labels_IO)

    unprocessed_data.rewind()
    i = 0
    for record in unprocessed_data:
        # Process the data
        try:
            if votes[i] == 'bad':
                raise Exception('Bad image:', img_urls[i])

            if votes[i]:
                indoor_collection.insert_one(record)
            else:
                outdoor_collection.insert_one(record)
        except:
            outdoor_collection.insert_one(record)
            outdoor_collection.update_one({"_id": record["_id"]}, {"$set": {"problematic": True}})

        # Update the original record with 'processed' flag
        source_collection.update_one({"_id": record["_id"]}, {"$set": {"processed": True, "source": "wikicommons"}})
        i += 1

    return len(img_urls)

def main(limitval=16):
    # load the labels
    labels_IO = load_labels()

    # load the model
    model = load_model()
    model.to(device)

    # Connect to MongoDB
    source_collection = get_collection()
    indoor_collection, outdoor_collection = get_in_out_collection()

    # Select all unprocessed data from the source collection
    while True:
        unprocessed_data = source_collection.find({"processed": {"$exists": False}}).limit(limitval)

        # Watch the specified collection for changes and process new documents
        number_processed = process_images(unprocessed_data, model, labels_IO, source_collection, indoor_collection, outdoor_collection)

        if not number_processed:
            break


if __name__ == "__main__":
    print(device)
    main(32)

