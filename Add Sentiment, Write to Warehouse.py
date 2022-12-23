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
        conn2 = psycopg2.connect(
            "host=sgdl1.cjf3sww93fr9.us-east-1.rds.amazonaws.com dbname=sgdl1 user=postgres password=1578SMDL")

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
        conn = psycopg2.connect(
            "host=kimstestdb.cujm2drdr40t.us-east-1.rds.amazonaws.com dbname=kimstestdb user=postgres password=315096KEK")

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

    # Iterate through the list of tweets from today and check if tweet is already in cleaned table in the data warehouse
    # If yes, skip
    # If no, add it to the tweets table in the data warehouse and add sentiment
    for tweet in tweets_today:
        if tweet[1] in id_list:
            pass
        # Conduct sentiment analysis
        else:
            print("starting sentiment analysis")
            text = (tweet[0])  # in each row only get the tweet text

            encoded_text = tokenizer(text, return_tensors='pt')

            output = model(**encoded_text)

            scores = output[0][0].detach().numpy()
            scores = softmax(scores)

            scores_dict = {
                'neg': scores[0],
                'neu': scores[1],
                'pos': scores[2]
            }
            # print(scores_dict)

            sentiment = max(scores_dict, key=scores_dict.get)


            r = (tweet[0], tweet[1], tweet[2], tweet[3], tweet[4], tweet[5],
                 sentiment)  # create a tuple out of each row and sentiment


            cur2.execute(
                """INSERT into tweets (text, id_tweet, lang, geo, capital, date, sentiment)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)""", r)

        attempts = 0
        success = False
        while attempts < 3 and not success:

            try:
                if tweet[1] in id_list:
                    pass
                else:
                    print("starting sentiment analysis")
                    text = (tweet[0])  # in each row only get the tweet text

                    encoded_text = tokenizer(text, return_tensors='pt')

                    output = model(**encoded_text)

                    scores = output[0][0].detach().numpy()
                    scores = softmax(scores)

                    scores_dict = {
                        'neg': scores[0],
                        'neu': scores[1],
                        'pos': scores[2]
                    }
                    # print(scores_dict)

                    sentiment = max(scores_dict, key=scores_dict.get)
                    print("got sentiment")

                    r = (tweet[0], tweet[1], tweet[2], tweet[3], tweet[4], tweet[5],
                         sentiment)  # create a tuple out of each row and sentiment


                    print("got r" + str(r))
                    cur2.execute(
                        """INSERT into tweets (text, id_tweet, lang, geo, capital, date, sentiment)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)""", r)
                    print(str(r))
                    success = True

            except:
                try:
                    conn2 = psycopg2.connect(
                        "host=sgdl1.cjf3sww93fr9.us-east-1.rds.amazonaws.com dbname=sgdl1 user=postgres password=1578SMDL")
                except psycopg2.Error as e:
                    print("Error: Could not make connection to the Postgres database")
                    print(e)

                try:
                    conn = psycopg2.connect(
                        "host=kimstestdb.cujm2drdr40t.us-east-1.rds.amazonaws.com dbname=kimstestdb user=postgres password=315096KEK")

                except psycopg2.Error as e:
                    print("Error: Could not make connection to the Postgres database")
                    print(e)
                cur = conn2.cursor()
                conn.set_session(autocommit=True)

                cur2 = conn2.cursor()
                conn2.set_session(autocommit=True)
                attempts += 1
                print("number of attempts:" + str(attempts))
                if attempts == 3:
                    print("had to break")
                    break


    postgreSQL_select_Query = "select * from tweets"
    cur2.execute(postgreSQL_select_Query)
    tweets = cur2.fetchall()
    for row in tweets:
        print(row)
