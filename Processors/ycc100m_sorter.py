# import sqlite3
# import yaml
# from multiprocessing import Pool
# from pymongo import MongoClient
# from places365.in_out_mongo import *
#
# with open('conf.yml', 'r') as configfile:
#     config = yaml.safe_load(configfile)
#
#
# # Update the database for a processed URL
# def update_database(conn, photoid, isindoor):
#     update_query = '''UPDATE yfcc100m_dataset SET isindoor = ? WHERE photoid = ?'''
#     cursor = conn.cursor()
#     cursor.execute(update_query, (isindoor, photoid))
#     conn.commit()
#
# # Process a single URL
# def process_record(record, conn, labels_IO, indoor_collection):
#     model = load_model()
#     model.to(device)
#     data = dict(record)
#     url = data['url']
#     id = data['photoid']
#     print(f"Processing URL: {url}")
#     try:
#         # Add your processing logic here
#         if is_indoor(url, model, labels_IO):
#             # Update database after processing
#             update_database(conn, id, isindoor=1)
#             indoor_collection.insert_one(data.update({'source': 'ycc100m'}))
#         else:
#             update_database(conn, id, isindoor=0)
#     except Exception as e:
#         #problematic
#         update_database(conn, id, isindoor=-1)
#
#
#
# # Function to fetch a batch of URLs with pagination
# def get_records_batch(db_path, offset=0, batch_size=1000):
#     conn = sqlite3.connect(db_path)
#     conn.row_factory = sqlite3.Row
#     cursor = conn.cursor()
#
#     while True:
#         query = '''SELECT uid as owner_name,
#            datetaken as date_taken,
#            dateuploaded as date_upload,
#            capturedevice,
#            title,
#            photoid,
#            description,
#            usertags as tags,
#            machinetags as machine_tags,
#            longitude,
#            latitude,
#            accuracy,
#            pageurl as page_url,
#            downloadurl as url,
#            licensename as license,
#            licenseurl,
#            ext as original_format,
#            isindoor FROM yfcc100m_dataset
#         WHERE isindoor is NULL and longitude IS NOT "" and latitude IS NOT ""
#         LIMIT ? OFFSET ?'''
#         cursor.execute(query, (batch_size, offset))
#         rows = cursor.fetchall()
#         if not rows:
#             break
#         #yield [row['your_url_column'] for row in rows]
#         yield rows
#         offset += batch_size
#
#     conn.close()
#
# def process_urls_in_batches(db_path, labels_IO, indoor_collection):
#     offset = 0
#     batch_size = 10000  # Adjust as needed based on memory and performance
#     total_records = []
#
#     while True:
#         records = get_records_batch(db_path, offset, batch_size)
#         if not records:
#             break
#         total_records.extend(records)
#         offset += batch_size
#
#     # Use multiprocessing pool to manage separate processes
#     with Pool(5) as pool:
#         pool.starmap(process_record, [(record, db_path, labels_IO, indoor_collection) for record in total_records])
#
#
# def get_indooor_collection():
#     # Create a connection using MongoClient.
#     client = MongoClient(config['mongo']['connection'])
#     return client[config['mongo']['processed']]['indoor']
#
# if __name__ == "__main__":
#     # load the labels
#     labels_IO = load_labels()
#
#     # Connect to MongoDB
#     indoor_collection = get_indooor_collection()
#     db_path = r'C:\Users\ogechi\Downloads\yfcc100m_dataset.sql'  # Path to your SQLite database
#     process_urls_in_batches(db_path, labels_IO, indoor_collection)

import sqlite3
import yaml
from multiprocessing import Pool
from pymongo import MongoClient
from places365.in_out_mongo import *

def load_config():
    with open('conf.yml', 'r') as configfile:
        return yaml.safe_load(configfile)

def get_indoor_collection(config):
    client = MongoClient(config['mongo']['connection'])
    db = client[config['mongo']['processed']]
    return db['indoor']

def update_database(conn, photoid, isindoor):
    update_query = '''UPDATE yfcc100m_dataset SET isindoor = ? WHERE photoid = ?'''
    cursor = conn.cursor()
    cursor.execute(update_query, (isindoor, photoid))
    conn.commit()

def process_record(args):
    data, db_path, labels_IO, config = args
    conn = sqlite3.connect(db_path)
    # Initialize MongoDB connection inside the process
    client = MongoClient(config['mongo']['connection'])
    indoor_collection = client[config['mongo']['processed']]['indoor']

    try:
        model = load_model()
        model.to(device)
        url = data['url']
        photoid = data['photoid']
        print(f"Processing URL: {url}")
        if is_indoor(url, model, labels_IO):
            update_database(conn, photoid, isindoor=1)
            data['source'] = 'yfcc100m'
            indoor_collection.insert_one(data)
        else:
            update_database(conn, photoid, isindoor=0)
    except Exception as e:
        update_database(conn, photoid, isindoor=-1)
        print(f"Error processing record {photoid}: {e}")
    finally:
        conn.close()
        client.close()

def get_records_batch(db_path, offset, batch_size):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        query = '''SELECT uid as owner_name,
                   datetaken as date_taken,
                   dateuploaded as date_upload,
                   capturedevice,
                   title,
                   photoid,
                   description,
                   usertags as tags,
                   machinetags as machine_tags,
                   longitude,
                   latitude,
                   accuracy,
                   pageurl as page_url,
                   downloadurl as url,
                   licensename as license,
                   licenseurl,
                   ext as original_format,
                   isindoor FROM yfcc100m_dataset 
                WHERE isindoor is NULL and longitude IS NOT "" and latitude IS NOT ""
                LIMIT ? OFFSET ?'''
        cursor.execute(query, (batch_size, offset))
        rows = cursor.fetchall()
        return rows
    finally:
        conn.close()

# Adjust how you call process_record in process_urls_in_batches
def process_urls_in_batches(db_path, labels_IO, config):
    batch_size = 10000  # Adjust as needed
    offset = 0
    while True:
        records = get_records_batch(db_path, offset, batch_size)
        if not records:
            break
        with Pool(5) as pool:
            pool.map(process_record, [(dict(record), db_path, labels_IO, config) for record in records])
        offset += batch_size


if __name__ == "__main__":
    config = load_config()
    labels_IO = load_labels()
    #indoor_collection = get_indoor_collection(config)
    db_path = r'C:\Users\ogechi\Downloads\yfcc100m_dataset.sql'  # Correct path to your SQLite database
    process_urls_in_batches(db_path, labels_IO, config)
