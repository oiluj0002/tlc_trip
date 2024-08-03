#Libraries
import os
import shutil
from google.cloud import storage
import requests

#Create directory for the downloaded data
if os.path.exists('src'):
    pass
else:
    os.mkdir('src')

#Import data from TLC NYC website
url1 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet'
url2 = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

response = requests.get(url1)
with open('src/trips.parquet', 'wb') as f:
    f.write(response.content)

response = requests.get(url2)
with open('src/taxi_zone.csv', 'wb') as f:
    f.write(response.content)

#Upload data to Google Cloud Storage
client = storage.Client()

bucket = client.bucket('tlc-trip-bronze')

blob_trips = bucket.blob('trips.parquet')
blob_taxi_zone = bucket.blob('taxi_zone.csv')

blob_trips.upload_from_filename('src/trips.parquet')
print(f"Arquivo trips.parquet enviado para o bucket tlc_trip_bronze")

blob_taxi_zone.upload_from_filename('src/taxi_zone.csv')
print(f"Arquivo taxi_zone.csv enviado para o bucket tlc_trip_bronze")

#Remove the downloaded data
shutil.rmtree('src')