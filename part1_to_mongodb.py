# https://kb.objectrocket.com/mongo-db/use-docker-and-python-for-a-mongodb-application-1046
# 1- Download MongoDB docker
# 2- Run docker: docker run -d -p 27017:27017 --name myname mongo
# container ID (512b7297d225): docker ps
# IP (172.17.0.2): docker inspect CONTAINER_ID
# import the MongoClient class

# create data directory
# https://hub.docker.com/_/mongo
# mounts the /my/own/datadir directory from the underlying host system as /data/db inside the container
# docker run --name some-mongo -v /my/own/datadir:/data/db -d mongo

import requests
import urllib.request
import urllib
import pandas as pd
import time
import dateutil.parser as dparser
from datetime import datetime
from bs4 import BeautifulSoup
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
from pymongo import MongoClient, errors

# global variables for MongoDB host (default port is 27017)
DOMAIN ='localhost'
PORT = 27017

# Input data URLs
urls = [
    "https://results.openaddresses.io/sources/es/25829",
    "https://results.openaddresses.io/sources/es/25830",
    "https://results.openaddresses.io/sources/es/25831",
    "https://results.openaddresses.io/sources/es/32628",
    "https://results.openaddresses.io/sources/es/nc/statewide"]


# use a try-except indentation to catch MongoClient() errors
try:
    # try to instantiate a client instance
    client = MongoClient(
        host=[str(DOMAIN) + ":" + str(PORT)],
        serverSelectionTimeoutMS=3000,
        # username="root",
        # password="example",
        connect=False,
    )

    # print the version of MongoDB server if connection successful
    print("server version:", client.server_info()["version"])

    # get the database_names from the MongoClient()
    database_names = client.list_database_names()
    # print("\n databases:", database_names)


except errors.ServerSelectionTimeoutError as err:
    # set the client and DB name list to 'None' and `[]` if exception
    client = None
    database_names = []

    # catch pymongo.errors.ServerSelectionTimeoutError
    print("pymongo ERROR:", err)

# Create a database in mongoDB
dblist = client.list_database_names()
if "es_addresses" in dblist:
    print("The database exists.")
else:
    mydb = client["es_addresses"]

# Initial value of the date of inserting data in mongodb
insert_date = datetime(2021, 1, 1).date()

# Continues update of mongoDB
while 1:

    # Read data for given URLs
    for url in urls:

        req = requests.get(url, allow_redirects=True)
        if req.status_code != 200:
            print('Request error. Can not read the web page.')
            continue

        soup = BeautifulSoup(req.content, 'html.parser')

        # check if more recent update exist
        # 1- Find updated date on the web page
        current_update_str = ''.join(soup.find('p').get_text().split()[-3:])
        try:
            current_update = dparser.parse(current_update_str, fuzzy=True).date()
        except:
            print('Error in finding the date of the last update .')
            continue

        # 2- check updates every hour
        if current_update <= insert_date:

            time.sleep(1)
            continue

        # 3- download request again

        # Get the url of the last updated output.zip
        url_zip = soup.find_all(class_='processed')[1].find('a').get('href')
        print('\n Loading data from ', url_zip)

        # Get file name from URL
        r = requests.get(url_zip)
        file_name = './data/' + url_zip.split('/')[-1]

        # Read output.zip file to dataframe and write it to MongoDB collection
        resp = urlopen(url_zip)
        url_zip_open = urllib.request.urlopen(url_zip)
        with ZipFile(BytesIO(url_zip_open.read())) as my_zip_file:
            for contained_file in my_zip_file.namelist():

                if contained_file.split('.')[1] == 'csv':

                    # read csv data to datafrmae
                    df = pd.read_csv(my_zip_file.open(contained_file), low_memory=False)

                    # Add the insert date to dataframe to be used later for the most recent update
                    insert_date = datetime.today().date()
                    df['INSERT_DATE'] = insert_date.strftime("%b %d %Y")

                    # Set collection name as the same as zip file name
                    col = contained_file.split('.')[0].replace('/', '_')

                    # Insert csv data to MongoDB collection
                    print('\n Inserting to MongoDB...')
                    client['es_addresses'][col].insert_many(df.to_dict(orient='records'))
                    print('Done.')

print('\n Data transfered to MongoDB.')
print('\n Collections: ', client['es_addresses'].list_collection_names())

# Extract mongoDB schema
schema = extract_pymongo_client_schema(client)
print('\n Database schema: ', schema['es_addresses'])
print(json.dumps(schema['es_addresses'], indent=4, sort_keys=True))
