import threading, time, sys, re
from datetime import datetime
import json
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto import Client, CAR, models
from atproto_client.models.utils import get_or_create
import mysql.connector
from atproto import exceptions
from mysql.connector import Error

BLOCK_COUNT = 0
DUPLICATE_COUNT = 0
UPDATE_PERIOD = 15
UPDATES_ENABLED = True

def fetchAuthFromFile(filename):
    with open(filename, 'r') as file:
        contents = file.read()

    username, password = contents.split(",")
    username = username.strip()
    password = password.strip()
    return [username,password]

# Fetch Auth info
bskyAuth = fetchAuthFromFile('auth.dat')

# firehoseClient is for reading the firehose, accClient is the list host
firehoseClient = FirehoseSubscribeReposClient()
accClient = Client()

# Connect initial account to bsky.
accClient.login(bskyAuth[0], bskyAuth[1])
print("Connected to bsky.")

# AT URI for the blocklist being used
blockList = "at://did:plc:dros7wdbcfosi5ablbi34prd/app.bsky.graph.list/3kyn24wd3v22x"

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

# Inserts a did value into the local DB so we don't double up blocks and waste requests
def insertRecord(connection, record):
    try:
        cursor = connection.cursor()
        sql_insert_query = """INSERT INTO allblock (did) VALUES (%s)"""
        cursor.execute(sql_insert_query, (record,))
        connection.commit()
    except Error as e:
        print(f"Error while inserting record: {e}")
        sys.exit("Problem with committing record to SQL.")

# Checks if a did exists in our local DB
def recordExists(connection, record):
    try:
        cursor = connection.cursor()
        sql_check_query = """SELECT COUNT(*) FROM allblock WHERE did = %s"""
        cursor.execute(sql_check_query, (record,))
        result = cursor.fetchone()
        return result[0] > 0
    except Error as e:
        print(f"Error while checking record: {e}")
        sys.exit("Problem checking DB for duplicate entry.")

# Pretty countdown    
def countdown_with_progress_bar(timeToWait, bar_length=30, updateRate=30):
    start_time = time.time()
    total_duration = timeToWait
    
    for i in range(timeToWait, 0, -updateRate):
        elapsed_time = time.time() - start_time
        remaining_time = max(timeToWait - elapsed_time, 0)
        progress = (total_duration - remaining_time) / total_duration
        arrow = '-' * int(round(progress * bar_length) - 1) + '>'
        spaces = ' ' * (bar_length - len(arrow))
        sys.stdout.write(f'\r[{arrow}{spaces}] {int(progress * 100)}% - {int(remaining_time)} seconds remain...')
        sys.stdout.flush()
        
        if remaining_time < updateRate:
            time.sleep(remaining_time)
        else:
            time.sleep(updateRate)
    
    # Final update for 100% completion
    sys.stdout.write(f'\r[{"-" * bar_length}] 100% - 0 seconds remain...\n')
    sys.stdout.flush()

# Attempts to block an account
def blockAccount(client, target, sqlConn):
    result = False

    try:
        # Attempt a block req
        result = client.app.bsky.graph.listitem.create(accClient._session.did, {"subject" : target, "list" : blockList, "createdAt" : client.get_current_time_iso()})
    
    except exceptions.RequestException as e:

        result = False

        # 429 is rate limit
        if e.args[0].status_code == 429:
            # fetch the rate limit reset timestamp.
            resetTime = int(e.args[0].headers['ratelimit-reset'])
            
            # Stop the firehose client while we wait.
            firehoseClient.stop()

            # Pause progress updates
            global UPDATES_ENABLED
            UPDATES_ENABLED = False

            # Find out how long it is until the reset is upon us, plus 30 seconds for safety.
            currentTimestamp = int(time.time())
            timeToWait = resetTime - currentTimestamp
            timeToWait + timeToWait + 60

            # Wait for it...
            print("Rate Limit Reached. Waiting " + str(timeToWait) + " seconds before continuing...")

            # Be kind and provide a countdown.
            countdown_with_progress_bar(timeToWait)

            # Start back up the firehose.
            firehoseClient.start(on_message_handler)

            # Turn back on updates
            UPDATES_ENABLED = True
    
    # If the result was done, commit to our local DB.
    if result != False:
        insertRecord(sqlConn, target)

# Quick and dirty JSON handler
class JSONExtra(json.JSONEncoder):
    def default(self, obj):
        try:
            result = json.JSONEncoder.default(self, obj)
            return result
        except:
            return repr(obj)

# Removes duplicates from a list (if someone mentions an account twice, for instance, or likes their own post)
def removeDuplicates(input_list):
    unique_list = list(set(input_list))
    return unique_list

# Stop handling after the preset
#def _stop_after_n_sec() -> None:
#    time.sleep(STOP_AFTER_SECONDS)
#    firehoseClient.stop()

# Print periodic progress updates
def CountReporting() -> None:
    global UPDATE_PERIOD
    global UPDATES_ENABLED
    global BLOCK_COUNT
    global DUPLICATE_COUNT

    if UPDATES_ENABLED:
        print("Blocked Accounts This Session: " + str(BLOCK_COUNT))
        print("Skipped Blocks (Duplicates) This Session: " + str(DUPLICATE_COUNT))

    threading.Timer(UPDATE_PERIOD, CountReporting).start()

# Handles response to content retrieved from firehose
def contentHandler(content, rawContent, source):
    content = json.dumps(rawContent, cls=JSONExtra, indent=2)
    
    # Matches did's in json blurbs
    handlePattern = r"(did\:plc\:.*?)\/"
 
    # Make a list that will contain all did's involved.
    idList = []

    # Populate it with the originator did
    idList.append(source)

    # Find did's referred in the content
    matches = re.findall(handlePattern, content)

    # Add them to the list and clear dupes
    idList.extend(matches)
    idList = removeDuplicates(idList)

    # Try to block each one.
    for thisID in idList:
        if recordExists(sqlConnection, thisID):
            global DUPLICATE_COUNT
            DUPLICATE_COUNT = DUPLICATE_COUNT + 1
        else:
            blockAccount(accClient, thisID, sqlConnection)
            global BLOCK_COUNT
            BLOCK_COUNT = BLOCK_COUNT + 1
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
            if cooked.py_type == "app.bsky.feed.repost": contentHandler(cooked, raw, commit.repo)
            if cooked.py_type == "app.bsky.feed.like": contentHandler(cooked, raw, commit.repo)
            if cooked.py_type == "app.bsky.feed.post": contentHandler(cooked, raw, commit.repo)

# Roll up the thread for stopping
#threading.Thread(target=_stop_after_n_sec).start()
CountReporting()

# Start the firehose
firehoseClient.start(on_message_handler)