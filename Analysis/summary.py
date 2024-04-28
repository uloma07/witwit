import pandas as pd

# Assuming you have a CSV file named 'data.csv' and you are interested in the column 'Column_Name'

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('../Data/processed/outdoor.csv')

# Count the unique entries in the column 'Column_Name'
unique_entries = df['url'].nunique()

# If you want a breakdown of counts for each unique entry
print(f"Number of unique entries in 'Column_Name': {unique_entries}")