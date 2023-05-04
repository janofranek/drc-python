import firebase_admin
from firebase_admin import firestore
import os
import fnmatch
from dotenv import dotenv_values
import json
import pandas

def process_excel_sheet(sheet, data, firestore_client):

    print(f"Processing sheet: {sheet}")

    # Convert dataframe to dictionary
    datadict = data.to_dict('records')

    # Store into firestore
    coll_ref = firestore_client.collection(sheet)

    for element in datadict:
        id = element.pop("id")
        doc_ref = coll_ref.document(id)
        doc_ref.set( element )

    return

def process_excel_file(fname, firestore_client):

    print(f"Processing file: {fname}")

    # Read Excel file using pandas
    try:
        xlsdata = pandas.read_excel(fname, sheet_name = None)
    except Exception as e:
        print(f"Error reading XLSX file: {e}")
        exit()

    # Each sheet represents collection
    for key in xlsdata:
        process_excel_sheet(key, xlsdata[key], firestore_client)

    return

def process_json_file(fname, firestore_client):

    print(f"Processing file: {fname}")

    #load file
    try:
        f = open(fname, "r", encoding="utf-8")
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
    coll_name = list(data)[0]
    coll_ref = firestore_client.collection(coll_name)

    for element in data[coll_name]:
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
    if os.path.isfile(fullname) and fnmatch.fnmatch(fname, "*.json"):
        process_json_file(fullname, firestore_client)
    if os.path.isfile(fullname) and fnmatch.fnmatch(fname, "*.xlsx"):
        process_excel_file(fullname, firestore_client)

