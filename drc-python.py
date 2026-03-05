import os
import sys
import fnmatch
import argparse

# Set environment variables before importing Firebase
# These help with gRPC thread cleanup issues
os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '0'
os.environ['GRPC_POLL_STRATEGY'] = 'poll'

from dotenv import dotenv_values
import firebase_admin
from firebase_admin import firestore
import json
import pandas
import warnings
import signal

# Suppress the specific threading warnings that occur during shutdown
warnings.filterwarnings("ignore", message=".*NoneType.*context manager.*", category=RuntimeWarning)

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
        sys.exit(1)

    # Each sheet represents collection
    for key in xlsdata:
        process_excel_sheet(key, xlsdata[key], firestore_client)

    return

def read_json_file(fname):
    # load file and read JSON with proper resource cleanup
    try:
        with open(fname, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Error reading JSON file: {e}")
                sys.exit(1)
    except Exception as e:
        print(f"Error opening file: {e}")
        sys.exit(1)

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

def delete_collection(coll_ref, batch_size):
    # helper for cleanup
    docs = coll_ref.limit(batch_size).stream()
    count = 0

    for doc in docs:
        doc.reference.delete()
        count = count + 1

    if count >= batch_size:
        return delete_collection(coll_ref, batch_size)

def restore_coll(config, coll_name, data, firestore_client):
    print(f"Restoring collection: {coll_name}")
    coll_ref = firestore_client.collection(coll_name)
    
    # Delete existing data
    delete_collection(coll_ref, 100)

    # Restore data
    for element in data:
        id = element.pop("id", None)
        if id:
            doc_ref = coll_ref.document(id)
            doc_ref.set(element)
    return

def restore_data(config, firestore_client):
    #find collections to process and then process them
    for fname in os.listdir(config["BACKUP_PATH"]):
        fullname = os.path.join(config["BACKUP_PATH"], fname)
        if os.path.isfile(fullname) and fnmatch.fnmatch(fname, "*.json"):
            # The collection name is the filename without extension
            coll_name = os.path.splitext(fname)[0]
            try:
                with open(fullname, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # The backup format is { "collection_name": [ ... data ... ] }
                    # We need to extract the list of data
                    rows = data.get(coll_name, [])
                    restore_coll(config, coll_name, rows, firestore_client)
            except Exception as e:
                print(f"Error restoring file {fname}: {e}")

def backup_courses(config, firestore_client):
    coll = firestore_client.collection("courses")
    print(f"Backing up collection: {coll.id}")
    backup_coll(config, coll)

def restore_courses(config, firestore_client):
    print("Restoring collection: courses")
    fname = "courses.json"
    fullname = os.path.join(config["BACKUP_PATH"], fname)
    if os.path.isfile(fullname):
        try:
            with open(fullname, 'r', encoding='utf-8') as f:
                data = json.load(f)
                rows = data.get("courses", [])
                restore_coll(config, "courses", rows, firestore_client)
        except Exception as e:
            print(f"Error restoring file {fname}: {e}")
    else:
        print(f"Backup file {fullname} not found.")

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

def generate_matches_2025(config, firestore_client):
    matches_ref = firestore_client.collection("matches")
    generate_matches_day(matches_ref, "2025-08-29", 6)
    generate_matches_day(matches_ref, "2025-08-30", 6)
    generate_matches_day(matches_ref, "2025-08-31", 12)

def transform(config, firestore_client):
    tournaments_ref = firestore_client.collection("tournaments")
    tournaments = tournaments_ref.stream()
    for tournament in tournaments:
        print(f"Transforming tournament: {tournament.id}")
        data = tournament.to_dict()
        updates = {}

        if "active" in data:
            old_active = data["active"]
            if not isinstance(old_active, bool):
                if old_active == "0" or old_active == 0:
                    new_active = False
                else:
                    new_active = True
                updates["active"] = new_active

        if "rounds" in data and isinstance(data["rounds"], list):
            rounds = data["rounds"]
            rounds_updated = False
            for r in rounds:
                if isinstance(r, dict) and "active" in r:
                    old_r_active = r["active"]
                    if not isinstance(old_r_active, bool):
                        if old_r_active == "0" or old_r_active == 0:
                            new_r_active = False
                        else:
                            new_r_active = True
                        
                        r["active"] = new_r_active
                        rounds_updated = True
            
            if rounds_updated:
                updates["rounds"] = rounds
        
        if updates:
            tournament.reference.update(updates)

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    print("\nReceived interrupt signal, cleaning up...")
    sys.exit(0)

if __name__ == "__main__":

  # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)

    #command line parameters
    parser = argparse.ArgumentParser(description='DRC Python Script')
    parser.add_argument('action', type=str, help='Action to perform')
    parser.add_argument('--env', type=str, default='test', choices=['test', 'prod'], help='Environment to use (test or prod)')
    
    args = parser.parse_args()
    action = args.action.lower()

    #read configuration
    env_file = f".env.{args.env}"
    print(f"Loading configuration from {env_file}")
    config = dotenv_values(env_file)

    #Firebase connection
    firestore_client = None
    app = None
    try:
        cred = firebase_admin.credentials.Certificate(config["FIREBASE_CRED"])
        app = firebase_admin.initialize_app(cred)
        firestore_client = firestore.client()
    except Exception as e:
        print(f"Error connecting to Firebase: {e}")
        sys.exit(1)

    # define the dictionary mapping cases to functions
    actions = {
        "load": load_data,
        "backup": backup_data,
        "restore": restore_data,
        "backup_courses": backup_courses,
        "restore_courses": restore_courses,
        "matches2023": generate_matches_2023,
        "matches2024": generate_matches_2024,
        "matches2025": generate_matches_2025,
        "transform": transform
    }

    # get the function corresponding to the case
    selected_action = actions.get(action, lambda: "Invalid case")

    # call the function and ensure Firebase app is cleaned up
    try:
        selected_action(config, firestore_client)
        print("Operation completed successfully")
        
    except Exception as e:
        print(f"Error during operation: {e}")
        
    finally:
        # Cleanup
        try:
            if firestore_client and hasattr(firestore_client, 'close'):
                firestore_client.close()
            if app:
                firebase_admin.delete_app(app)
        except:
            pass
        
        # Force exit to avoid threading cleanup issues
        os._exit(0)
