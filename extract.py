from faker import Faker
from google.cloud import storage
import csv
import numpy as np
import pandas as pd

# Number of records to generate
num_records = 1000

# Initialize Faker
fake = Faker()

# Cities and product categories
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
product_categories = ['Electronics', 'Furniture', 'Clothing', 'Groceries', 'Toys', 'Books', 'Beauty', 'Sports']

# Create and open a CSV file
with open('sales_data.csv', mode='w', newline='') as file:
    fieldnames = ['Date', 'Customer_ID', 'Customer_first_name', 'Customer_last_name', 'email', 'phone_number', 'Order_ID', 'Product_ID', 'Product_Category', 'Quantity_Sold', 'Price_per_Unit', 'Discount', 'City']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    
    # Write the header
    writer.writeheader()

    # Generate records
    for i in range(num_records):
        writer.writerow({
            'Date': fake.date_between(start_date='-1y', end_date='today'),
            'Customer_ID': np.random.randint(1000, 2000),
            'Customer_first_name': fake.first_name(),
            'Customer_last_name': fake.last_name(),
            'email': fake.email(),
            'phone_number': ''.join([str(np.random.randint(0, 10)) for _ in range(10)]),
            'Order_ID': i + 1,
            'Product_ID': np.random.randint(100, 500),
            'Product_Category': np.random.choice(product_categories),
            'Quantity_Sold': np.random.randint(1, 10),
            'Price_per_Unit': round(np.random.uniform(10.0, 1500.0), 2),
            'Discount': round(np.random.uniform(0, 90), 2),
            'City': np.random.choice(cities)
        })

# Function to upload the file to Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

# Set your GCS bucket information
bucket_name = 'sales-bucket-1'
source_file_name = 'sales_data.csv'
destination_blob_name = 'sales_data.csv'

# Upload the file to GCS
upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
