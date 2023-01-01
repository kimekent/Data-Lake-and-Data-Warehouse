This repository contains the following files: 

Twitter Files: 

get_tweets_apache.py: Run by the tweets dag. This python script gets the tweets.

Add Sentiment, Write to Ware House.py: Run by the sentiment dag. This python script does conducts the duplicate check and sentiment analysis and writes the non duplicate tweets with their sentiment to the data warehouse. 

average.py: Run by the average dag. This python script creates an aggregated table by city  out of the tweets table in the data warehouse.

twitter_dag.py:  Script that runs the "get_tweets_apache.py" script.

sentiment_dagt.py: Script that runs the "Add Sentiment, Write to Ware House.py" script.

average_dag.py: Script that runs the "average.py" script.



Weather Files: 

weather_dag.py: combining the two python scripts below to execute them sequentially. 

get_weather_apache.py: Contains the code to create a table within the data lake and fetch the weather data to store in a table

send_to_dwh_apache: Containing the code to create a table in the data warehouse and extract data frome the data lake to move it to the warehouse. 


Cost Files:

scrape_Script_clp.py: scrapes each city and country and for each one it creates one file. E.g. Brussels_Belgium.json, etc.
scrape_all_in_one.py: scrapes each city and country but saves all responses in one json file. E.g. scrape_costlivingprice.json

lambda_function_s3_to_rds.py: imports/inserts the "cost_living_prices_europe_cleanV2.csv" from the s3bucket into the data warehouse
Connect_to_dw.ipynb: connects to data warehouse and prints the header of "costlivingprices" table
