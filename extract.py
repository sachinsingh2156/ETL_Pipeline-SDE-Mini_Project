from faker import Faker
import random
import string
from google.cloud import storage
import csv

num_employee = 100

fake = Faker()
employee_data = []
departments = ['Engineering', 'Sales', 'Human Resources', 'Finance', 'Marketing', 'Product Development']
password_characters = string.ascii_letters + string.digits + 'm'

with open('employee_data.csv',mode='w', newline='') as file:
    fieldnames = ['first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    
    writer.writeheader()
    for _ in range (num_employee):
        writer.writerow({
            "first_name" : fake.first_name(),
            "last_name" : fake.last_name(),
            "job_title" : fake.job(),
            "department" : random.choice(departments),
            "email" : fake.email(),
            "address" : fake.address(),
            "phone_number" : fake.phone_number(),
            "salary" : fake.random_number(digits=5),
            "password" : ''.join(random.choice(password_characters) for _ in range (8))
        })

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

bucket_name = 'employee-data-bucket1'
source_file_name = 'employee_data.csv'
destination_blob_name = 'employee_data.csv'

upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
