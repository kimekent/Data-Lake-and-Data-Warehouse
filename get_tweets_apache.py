# Import Packages
import tweepy
import psycopg2
from datetime import date
import pandas as pd

# Function that is run by get tweets dag
def run_get_tweets():
    # Twitter Credentials
    client = tweepy.Client(bearer_token = credentials[bearer_token])
    auth = tweepy.OAuthHandler("IpWL6YCf7acBj8BrJPcDZ7SmH", "ANaBxuDPbFSZPlrI3v94TPQYXHcDuNkDG7T54sftQCibCSIxLI")
    auth.set_access_token("1580960869172887557-9Ui3It9gwYiv6UeU2wXn6ZSN9Uz2dB", "61BdNnKj17jMas8K4ksP8b7ReY1Wdqh6FJpfRe3Nwajms")
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # Connect to RDS in data lake and create twitter table
    try:
        from connect1 import dl_credentials

        con = dl_credentials()


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
