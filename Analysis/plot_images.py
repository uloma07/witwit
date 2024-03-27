import pandas as pd
from pymongo import MongoClient
import yaml
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

with open('../conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)
def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['flickr']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection


item_details = get_collection().find()
entries = list(item_details)
df = pd.DataFrame(entries)
# Load the CSV file into a DataFrame
#df = pd.read_csv('../Data/raw/image_data.csv')

# Extract latitude and longitude columns
lats = pd.to_numeric(df['latitude']).tolist()
lons = pd.to_numeric(df['longitude']).tolist()

# Create a map
plt.figure(figsize=(10, 8))
m = Basemap(projection='mill', llcrnrlat=-90, urcrnrlat=90, llcrnrlon=-180, urcrnrlon=180, resolution='c')

# Draw coastlines, countries, and states
m.drawcoastlines()
m.drawcountries()
m.drawstates()

# Convert latitude and longitude to map coordinates
x, y = m(lons, lats)

# Plot the data points
m.scatter(x, y, s=20, color='red', alpha=0.5)

# Add a title
plt.title('Distribution of Data Points')

# Show the plot
plt.show()
