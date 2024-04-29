from pymongo import MongoClient
import yaml
import time
from places365.in_out_mongo import *


with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)


def get_in_out_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    indoor_collection = client[config['mongo']['processed']]['indoor']
    outdoor_collection = client[config['mongo']['processed']]['outdoor']
    return indoor_collection, outdoor_collection


def get_problematic_out_docs():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    outdoor_collection = client[config['mongo']['processed']]['outdoor']
    out_data = outdoor_collection.find({"problematic": {"$exists": True}})
    return out_data


def process_images(records, model, labels_IO, indoor_collection, outdoor_collection):
    # Select all unprocessed data from the source collection
    for record in records:
        # Process the data
        try:
            if is_indoor(record['url'], model, labels_IO):
                indoor_collection.insert_one(record)
                indoor_collection.update_one({"_id": record["_id"]}, {"$unset": {"problematic": 1}})
                outdoor_collection.delete_one({"_id": record["_id"]})
            else:
                outdoor_collection.update_one({"_id": record["_id"]}, {"$unset": {"problematic": 1}})
        except Exception as e:
            print(e, record['url'])
            continue

def main():
    # load the labels
    labels_IO = load_labels()

    # load the model
    model = load_model()
    model.to(device)

    # Connect to MongoDB
    records = get_problematic_out_docs()
    indoor_collection, outdoor_collection = get_in_out_collection()

    # Watch the specified collection for changes and process new documents
    process_images(records, model, labels_IO, indoor_collection, outdoor_collection)



if __name__ == "__main__":
    print(device)
    main()

