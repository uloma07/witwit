from pymongo import MongoClient
import yaml
from places365.in_out_mongo import *

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)


def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['flickr']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection

def get_in_out_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    indoor_collection = client[config['mongo']['processed']]['indoor']
    outdoor_collection = client[config['mongo']['processed']]['outdoor']
    return indoor_collection, outdoor_collection


def process_images(source_collection, model, labels_IO, indoor_collection, outdoor_collection):
    # Select all unprocessed data from the source collection
    #unprocessed_data = source_collection.find({"processed": {"$exists": False}, "license": {"$ne": "0"}})
    #{processed: {$exists: false}, license: {$not: {$ in: ["6", "7", "9", "8", "10"]}}}
    unprocessed_data = source_collection.find({"processed": {"$exists": False}, "license": {"$in": ["6", "7", "9", "8", "10"]}})
    #unprocessed_data = source_collection.find({"processed": {"$exists": False}, "license": "5"})
    unprocessed_data = source_collection.find({"processed": {"$exists": False}, "license": {"$in": ["3", "4", "5"]}})

    for record in unprocessed_data:
        # Process the data
        try:
            if is_indoor(record['url'], model, labels_IO):
                indoor_collection.insert_one(record)
            else:
                outdoor_collection.insert_one(record)
        except:
            outdoor_collection.insert_one(record)
            outdoor_collection.update_one({"_id": record["_id"]}, {"$set": {"problematic": True}})

        # Update the original record with 'processed' flag
        source_collection.update_one({"_id": record["_id"]}, {"$set": {"processed": True}})


def main():
    # load the labels
    labels_IO = load_labels()

    # load the model
    model = load_model()
    model.to(device)

    # Connect to MongoDB
    collection = get_collection()
    indoor_collection, outdoor_collection = get_in_out_collection()

    # Watch the specified collection for changes and process new documents
    process_images(collection, model, labels_IO, indoor_collection, outdoor_collection)



if __name__ == "__main__":
    print(device)
    main()
