import threading, time, sys, re
from datetime import datetime
import json
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto import Client, CAR, models
from atproto_client.models.utils import get_or_create
import mysql.connector
from atproto import exceptions
from mysql.connector import Error

UPDATE_PERIOD = 10
UPDATES_ENABLED = True
FOUND_COUNT = 0
DUPLICATE_COUNT = 0

def fetchAuthFromFile(filename):
    with open(filename, 'r') as file:
        contents = file.read()

    username, password = contents.split(",")
    username = username.strip()
    password = password.strip()
    return [username,password]

# Connects to local SQL server
def mysqlConnect(auth):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='bsky',
            user=sqlAuth[0],
            password=sqlAuth[1]
        )
        if connection.is_connected():
            print("Connected to local DB.")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        sys.exit("Could not connect to local DB.")

# Initialize local SQL connection
sqlAuth = fetchAuthFromFile('sqlAuth.dat')
sqlConnection = mysqlConnect(sqlAuth)

# firehoseClient is for reading the firehose
firehoseClient = FirehoseSubscribeReposClient()

# Inserts a did value into the local DB for future blocking
def addToQueue(connection, record):
    try:
        cursor = connection.cursor()
        
        sql_insert_query = """INSERT INTO allblock (did, isBlocked) VALUES (%s, %s)"""
        data = (record, 0)
        
        cursor.execute(sql_insert_query, data)
        connection.commit()
    except Error as e:
        print(f"Error while inserting record: {e}")
        sys.exit("Problem with committing record to SQL.")

# Checks if a did exists in our local DB
def didAlreadyFound(connection, record):
    try:
        cursor = connection.cursor()
        sql_check_query = """SELECT COUNT(*) FROM allblock WHERE did = %s"""
        cursor.execute(sql_check_query, (record,))
        result = cursor.fetchone()
        return result[0] > 0
    except Error as e:
        print(f"Error while checking record: {e}")
        sys.exit("Problem checking DB for duplicate entry.")

# Quick and dirty JSON handler
class JSONExtra(json.JSONEncoder):
    def default(self, obj):
        try:
            result = json.JSONEncoder.default(self, obj)
            return result
        except:
            return repr(obj)

# Removes duplicates from a list (if someone mentions an account twice, for instance, or likes their own post)
def removeDuplicates(inputList):
    uniqueList = list(set(inputList))
    return uniqueList

# Print periodic progress updates
def countReporting() -> None:
    global UPDATE_PERIOD
    global UPDATES_ENABLED
    global FOUND_COUNT
    global DUPLICATE_COUNT

    if UPDATES_ENABLED:
        print("DID's added to queue this session: " + str(FOUND_COUNT))
        print("Duplicates found this session: " + str(DUPLICATE_COUNT))

    threading.Timer(UPDATE_PERIOD, countReporting).start()

# Handles response to content retrieved from firehose
def contentHandler(rawContent, source):
    global sqlConnection
    handlePattern = r"(did\:plc\:.*?)\/"        # Matches did's in json blurbs
    idList = []                                 # Make a list that will contain all did's involved.

    content = json.dumps(rawContent, cls=JSONExtra, indent=2)
    
    # Populate it with the originator did
    idList.append(source)

    # Find did's referred in the content
    matches = re.findall(handlePattern, content)

    # Add them to the list and clear dupes
    idList.extend(matches)
    idList = removeDuplicates(idList)

    # Try to block each one.
    for thisID in idList:
        if didAlreadyFound(sqlConnection, thisID):
            global DUPLICATE_COUNT
            DUPLICATE_COUNT = DUPLICATE_COUNT + 1
        else:
            addToQueue(sqlConnection, thisID)
            global FOUND_COUNT
            FOUND_COUNT = FOUND_COUNT + 1
    return

# Firehose message handler
def on_message_handler(message):
    commit = parse_subscribe_repos_message(message)

    # If it isn't a commit, ignore it
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    
    # Parse it
    car = CAR.from_bytes(commit.blocks)

    # For each operation in the commit...
    for op in commit.ops:
        # If it's a create (new record) in atproto...
        if op.action in ["create"] and op.cid:
            # Prep the data...
            raw = car.blocks.get(op.cid)
            cooked = get_or_create(raw, strict=False)
            
            # Handle only the relevant types (reposts, likes, posts)
            if cooked.py_type == "app.bsky.feed.repost": contentHandler(raw, commit.repo)
            if cooked.py_type == "app.bsky.feed.like": contentHandler(raw, commit.repo)
            if cooked.py_type == "app.bsky.feed.post": contentHandler(raw, commit.repo)


# Begin Thread for reporting progress
countReporting()

# Start the firehose
firehoseClient.start(on_message_handler)