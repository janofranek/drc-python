import firebase_admin
from firebase_admin import firestore
import os
import fnmatch
from dotenv import dotenv_values
import json


def process_file(fname, firestore_client):

    print(f"Processing file: {fname}")

    #load file
    try:
        f = open(fname, "r")
    except Exception as e:
        print(f"Error opening file: {e}")
        exit()

    #read json
    try:
        data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        exit()

    #first key is the collection name
    colname = list(data)[0]
    coll_ref = firestore_client.collection(colname)

    for element in data[colname]:
        id = element.pop("id")
        doc_ref = coll_ref.document(id)
        doc_ref.set( element )

    return

#MAIN

#read configuration
config = dotenv_values(".env")

#Firebase connection
try:
    cred = firebase_admin.credentials.Certificate(config["FIREBASE_CRED"])
    firebase_admin.initialize_app(cred)
    firestore_client = firestore.client()
except Exception as e:
    print(f"Error connecting to Firebase: {e}")
    exit()

#find JSON files to process
for fname in os.listdir(config["DATA_PATH"]):
    fullname = os.path.join(config["DATA_PATH"], fname)
    if os.path.isfile(fullname) and fnmatch.fnmatch(fname, config["DATA_PATTERN"]):
        output = process_file(fullname, firestore_client)

