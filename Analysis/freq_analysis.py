import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV files into DataFrames
df_images = pd.read_csv('../Data/raw/image_data.csv')
df_cities = pd.read_csv('../Data/raw/worldcities.csv')

# Perform an inner join to merge the DataFrames based on the 'location_id' and 'id' columns
merged_df = pd.merge(df_images, df_cities[['id', 'city']], left_on='location_id', right_on='id')

# Use groupby to get the count of images per city
city_counts = merged_df.groupby('city').size().reset_index(name='count')

# Plot the results
plt.figure(figsize=(10, 6))
plt.bar(city_counts['city'], city_counts['count'])
plt.title('Number of Images per City')
plt.xlabel('City')
plt.ylabel('Number of Images')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()