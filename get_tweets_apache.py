# Import Packages
import tweepy
import psycopg2
from datetime import date
import pandas as pd

# Function that is run by get tweets dag
def run_get_tweets():
    # Twitter Credentials
    client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAABywiAEAAAAAfYcaK69%2Bs1m9LTMAr2rxv54scWM%3DXMQMOYL4K5CG9y10wRAUMwaLZCASaad1iQESXzcYpgXSvmPMHZ')
    auth = tweepy.OAuthHandler("IpWL6YCf7acBj8BrJPcDZ7SmH", "ANaBxuDPbFSZPlrI3v94TPQYXHcDuNkDG7T54sftQCibCSIxLI")
    auth.set_access_token("1580960869172887557-9Ui3It9gwYiv6UeU2wXn6ZSN9Uz2dB", "61BdNnKj17jMas8K4ksP8b7ReY1Wdqh6FJpfRe3Nwajms")
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # Connect to RDS in data lake and create twitter table
    try:
        conn = psycopg2.connect("host=kimstestdb.cujm2drdr40t.us-east-1.rds.amazonaws.com dbname=kimstestdb user=postgres password=315096KEK")

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    conn.set_session(autocommit=True)
    cur.execute("""CREATE TABLE IF NOT EXISTS tweets (
                text varchar(3000), tweet_id BIGINT,
                lang varchar(3000), geo varchar(3000), capital varchar(3000), date_today DATE);""")


    # Create list of capitals we want to search for on twitter
    capitals = []
    cur.execute(""" select capital from capitals; """)
    for city in cur.fetchall():
        city = city[0]
        capitals.append(city)
        print(city)

    # Iterate through capitals list and get tweets with # of capital
    for capital in capitals:
        query = "#{x} lang:en -is:retweet".format(x=capital) # only get english tweets and no retweets

        try:
            response = client.search_recent_tweets(query=query, max_results=10, tweet_fields=['created_at', "lang", "geo"])
        except tweepy.errors.BadRequest:
            print("bad request")
            pass
        try:
            for tweet in response.data:
                text = tweet.text
                tweet_id = tweet.id
                lang = tweet.lang
                geo = tweet.geo
                capital = capital
                date_today = date.today().strftime("%Y-%m-%d")

                # Insert tweets into tweets table in data lake.
                cur.execute(
                    "INSERT INTO tweets (text, tweet_id, lang, geo, capital, date_today) VALUES (%s, %s, %s, %s, %s, %s)",
                    (text, tweet_id, lang, geo, capital, date_today))
                print("success")

        except:
            pass


    cur.close()
    conn.close()

    # SQL Commands
    #Show all tables
    cur2.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    for table in cur2.fetchall():
        print(table)

    # Print all rows in a table
    postgreSQL_select_Query = "select * from weatherforecast"
    cur.execute(postgreSQL_select_Query)
    tweets = cur.fetchall()
    for row in tweets:
        print(row)

    dropTableStmt = "DROP TABLE %s;" % "tweets";
    cur2.execute(dropTableStmt)

    count_rows = """ SELECT COUNT (*) FROM tweets;"""
    cur.execute(count_rows)
    table_count = cur.fetchone()
    print(table_count)

# Delete all rows except cardiff
DELETE = """DELETE FROM tweets
            WHERE col5 != 'Cardiff' """
cur.execute(DELETE)

get_column_name ="""SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = N'capitals'"""

change_column_name = """ALTER TABLE tweets
  RENAME COLUMN 0 TO text;"""
cur.execute(change_column_name)

cur2.execute("Select * FROM costlivingprices LIMIT 0")
colnames = [desc[0] for desc in cur.description]
colnames

pd.set_option('display.max_columns', None)
data = pd.DataFrame(tweets)
data.head()
