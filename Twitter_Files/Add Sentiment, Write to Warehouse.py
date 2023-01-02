# this is the Function that is run by the sentiment dag
# 1. removes all duplicate tweets.
# 2. Conducts sentiment analysis of the new tweets added to the data lake.
# 3. Copies the new tweets and their sentiment to the data warehouse.

def run_sentiment():
    # import packages
    import psycopg2
    from transformers import AutoTokenizer
    from transformers import AutoModelForSequenceClassification
    from scipy.special import softmax

    # Define model used for the sentiment analysis
    MODEL = f"cardiffnlp/twitter-roberta-base-sentiment"
    tokenizer = AutoTokenizer.from_pretrained(MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL)

    # Connect to data warehouse
    try:
        from connect2 import dw_credentials

        con2 = dw_credentials()

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
    try:
        cur2 = conn2.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn2.set_session(autocommit=True)

    # Create new table tweets in the data warehouse
    cur2.execute("""CREATE TABLE IF NOT EXISTS tweets(text varchar(3000), id_tweet BIGINT,
                lang varchar(3000), geo varchar(3000), capital varchar(3000), date DATE,
                sentiment varchar(3000));""")

    # Create list with tweet ids of all tweets already in the data warehouse table
    select_cleaned_id = """SELECT id_tweet FROM tweets"""
    cur2.execute(select_cleaned_id)
    ids = cur2.fetchall()
    id_list = []
    for id in ids:
        id = id[0]
        id_list.append(id)

    # Connect to RDS in data lake
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

    # Get all tweets scraped today
    get_tweets_today = """SELECT * FROM tweets WHERE date_today >= CURRENT_DATE;"""
    cur.execute(get_tweets_today)
    tweets_today = cur.fetchall()

    # Iterate through the list of tweets from today and
    # check if tweet is already in cleaned table in the data warehouse
    # If yes, skip
    # If no, add it to the tweets table in the data warehouse and add sentiment
    for tweet in tweets_today:
        if tweet[1] in id_list:
            pass
        else:
            # Conduct sentiment analysis
            text = (tweet[0])  # in each row only get the tweet text
            encoded_text = tokenizer(text, return_tensors='pt') # encoding text
            output = model(**encoded_text) # returns a tensor
            scores = output[0][0].detach().numpy() # transform to numpy for softmax transformation
            scores = softmax(scores) # apply softmax for better interpretability
            scores_dict = {                 # scores are stored in dictionary
                'neg': scores[0],
                'neu': scores[1],
                'pos': scores[2]
            }
            sentiment = max(scores_dict, key=scores_dict.get)
            r = (tweet[0], tweet[1], tweet[2], tweet[3], tweet[4], tweet[5],
                 sentiment)  # create a tuple out of each tweet and sentiment


            cur2.execute(
                """INSERT into tweets (text, id_tweet, lang, geo, capital, date, sentiment)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)""", r)
