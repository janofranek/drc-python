import os
import sys
import fnmatch
from dotenv import dotenv_values
import firebase_admin
from firebase_admin import firestore
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

def read_json_file(fname):
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

    return data

def process_json_file(fname, firestore_client):

    print(f"Processing file: {fname}")

    #read JSON file into dictionary
    data = read_json_file(fname)

    #first key is the collection name
    coll_name = list(data)[0]
    coll_ref = firestore_client.collection(coll_name)

    for element in data[coll_name]:
        id = element.pop("id")
        doc_ref = coll_ref.document(id)
        doc_ref.set( element )

    return

def load_data(config, firestore_client):
    #find files to process and then process them
    for fname in os.listdir(config["DATA_PATH"]):
        fullname = os.path.join(config["DATA_PATH"], fname)
        if os.path.isfile(fullname) and fnmatch.fnmatch(fname, "*.json"):
            process_json_file(fullname, firestore_client)
        if os.path.isfile(fullname) and fnmatch.fnmatch(fname, "*.xlsx"):
            process_excel_file(fullname, firestore_client)

def backup_coll(config, coll):
    #backup one collection
    fullname = os.path.join(config["BACKUP_PATH"], coll.id + ".json")
    docs = coll.get()
    data = []
    for doc in docs:
        doc_data = doc.to_dict()
        doc_data['id'] = doc.id
        data.append(doc_data)
    backup = {}
    backup[coll.id] = data
    with open(fullname, 'w') as file:
        json.dump(backup, file, indent=4)

def backup_data(config, firestore_client):
    #find collections to process and then process them
    for coll in firestore_client.collections():
        backup_coll(config, coll)

def generate_scorecards(config, firestore_client):
    tournaments = read_json_file(os.path.join(config["DATA_PATH"], "tournaments.json"))
    scorecards_ref = firestore_client.collection("scorecards")
    holes = []
    for i in range(18): 
        holes.append({ "hole": i+1, "score": 0})
    for r in tournaments[0].rounds:
        for p in tournaments[0].players:
            doc_ref = scorecards_ref.document(r.date + " " + p)
            #TODO - insert only if not exists 
            doc_ref.set( { "course": r.course, "player": p, "tee": "yellow", "holes": holes} )

    return

def generate_matches_day(matches_ref, date, matches_count):
    empty_match = { "holes": [], "players_lat": [], "players_stt": [], "final": False, "result": "", "final_score": 0 }
    for i in range(matches_count):
        doc_ref = matches_ref.document(date + "-" + f"{i:02d}")
        doc_ref.set(empty_match)


def generate_matches_2023(config, firestore_client):
    matches_ref = firestore_client.collection("matches")
    generate_matches_day(matches_ref, "2023-09-01", 6)
    generate_matches_day(matches_ref, "2023-09-02", 6)
    generate_matches_day(matches_ref, "2023-09-03", 11)

def generate_matches_2024(config, firestore_client):
    matches_ref = firestore_client.collection("matches")
    generate_matches_day(matches_ref, "2024-08-30", 6)
    generate_matches_day(matches_ref, "2024-08-31", 6)
    generate_matches_day(matches_ref, "2024-09-01", 11)

#MAIN

#command line parameters
if len(sys.argv) < 2:
    print("Missing argument")
    exit()
    
action = sys.argv[1].lower()

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

# define the dictionary mapping cases to functions
actions = {
    "load": load_data,
    "backup": backup_data,
    "scorecards": generate_scorecards,
    "matches2023": generate_matches_2023,
    "matches2024": generate_matches_2024
}

# get the function corresponding to the case
selected_action = actions.get(action, lambda: "Invalid case")

# call the function
selected_action(config, firestore_client)

