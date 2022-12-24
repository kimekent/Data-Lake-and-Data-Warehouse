# This function is run by the tableau dag.
# It creates a table that only contains the necessary and aggregated date for the visualisation in Tableau

# Connect to data lake
from connect2 import dw_credentials

con2 = database_credentials()

except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)
try:
    cur2 = conn2.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)
conn2.set_session(autocommit=True)

# Create new table sentiment_agr in the data warehouse
dropTableStmt = "DROP TABLE %s;" % "sentiment_agr";
cur2.execute(dropTableStmt)

# Create table sentiment_agr
cur2.execute("""CREATE TABLE IF NOT EXISTS sentiment_agr(capital varchar(3000),
            count_tweets int, POS int, NEG int, NEU int, positivity_score int, sentiment varchar(3000));""")

# Count the number of positive, negative and neutral tweets be country.
group_by_country = """select capital,
        count(*) as count_tweets,
       sum(case when sentiment = 'pos' then 1 else 0 end) as POS,
       sum(case when sentiment = 'neg' then 1 else 0 end) as NEG,
       sum(case when sentiment = 'neu' then 1 else 0 end) as NEU
       
        
        FROM tweets
        GROUP BY capital;"""

cur2.execute(group_by_country)
sentiments = cur2.fetchall()

# Create a happiness score, here called positivity score
# Happiness score = (positive number of tweets / total number of tweets about country) * 100
for row in sentiments:
    avg = (row[2] / (row[2] + row[3] + row[4]))*100
    if avg <= 30:
        sentiment = "negative"
    elif 30 < avg <= 60:
        sentiment = "neutral"
    elif avg > 60:
        sentiment = "positive"
    l = list(row)
    l.append(avg)
    l.append(sentiment)
    row = tuple(l)

    cur2.execute("""INSERT into sentiment_agr(capital, count_tweets, POS, NEG, NEU, positivity_score, sentiment)
                             VALUES (%s, %s, %s, %s, %s, %s, %s)""", row)


postgreSQL_select_Query = "select * from sentiment_agr"
cur2.execute(postgreSQL_select_Query)
tweets = cur2.fetchall()
for row in tweets:
    print(row)
count_rows = """ SELECT COUNT (*) FROM sentiment_agr;"""
cur2.execute(count_rows)
table_count = cur2.fetchone()
print(table_count)
    dropTableStmt = "DROP TABLE %s;" % "sentiment_agr";
    cur2.execute(dropTableStmt)
