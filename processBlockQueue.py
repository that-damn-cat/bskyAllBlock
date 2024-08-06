import threading, time, sys, re
from datetime import datetime

from atproto import Client, CAR, models
from atproto import exceptions

import mysql.connector
from mysql.connector import Error

from pprint import pprint

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
    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")
        sys.exit()

# Marks a given DID value in the database as Blocked.
def markAsBlocked(connection, thisDID):
    try:
        cursor = connection.cursor()
        updateQuery = """UPDATE allblock SET isBlocked = 1 WHERE did = %s"""
        cursor.execute(updateQuery, (thisDID,))
        connection.commit()
        cursor.close()
    except mysql.connector.Error as e:
        print(f"Error while setting blocked status: {e}")
        sys.exit()

# Fetches the number of DIDs in the database that haven't been blocked yet
def sqlQueueSize(connection):
    try:
        cursor = connection.cursor()
        countQuery = """SELECT COUNT(*) FROM allblock WHERE isBlocked = 0"""
        cursor.execute(countQuery)
        result = cursor.fetchone()
        cursor.close()

        return int(result[0])
    except mysql.connector.Error as e:
        print(f"Error while fetching queue size: {e}")
        sys.exit()

# Prefetches part of the block queue
def fetchBlockList(connection, count):
    try:
        cursor = connection.cursor()
        fetchQuery = """SELECT did FROM allblock WHERE isBlocked = 0 ORDER BY id ASC LIMIT %s"""
        cursor.execute(fetchQuery, (count,))
        result = cursor.fetchall()
        cursor.close()

        localList = [row[0] for row in result]
        return localList
    except mysql.connector.Error as e:
        print(f"Error while fetching block queue: {e}")
        sys.exit()

# Pretty countdown    
def SleepWithCountdownProgressBar(timeToWait, bar_length=30, updateRate=15):
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

        # 429 is rate limit
        if e.args[0].status_code == 429:
            # fetch the rate limit reset timestamp.
            resetTime = int(e.args[0].headers['ratelimit-reset'])

            # Find out how long it is until the reset is upon us, plus 30 seconds for safety.
            currentTimestamp = int(time.time())
            timeToWait = resetTime - currentTimestamp
            timeToWait + timeToWait + 30

            # Wait for it...
            print("Rate Limit Reached. Waiting " + str(timeToWait) + " seconds before continuing...")

            # Be kind and provide a countdown.
            SleepWithCountdownProgressBar(timeToWait)
    
    else:
        if result != False:
            pprint(result)
            markAsBlocked(sqlConn, target)

# Fetch Auth info, connect to bsky
bskyAuth = fetchAuthFromFile('auth.dat')
accClient = Client()
accClient.login(bskyAuth[0], bskyAuth[1])
print("Connected to bsky.")

# AT URI for the blocklist being used
blockList = "at://did:plc:dros7wdbcfosi5ablbi34prd/app.bsky.graph.list/3kyn24wd3v22x"    

# Initialize local SQL connection
sqlAuth = fetchAuthFromFile('sqlAuth.dat')
sqlConnection = mysqlConnect(sqlAuth)

allBlocked = False

while not allBlocked:
    localList = fetchBlockList(sqlConnection, 200)

    for item in localList:
        blockAccount(accClient, item, sqlConnection)

    allBlocked = (sqlQueueSize(sqlConnection) <= 0)