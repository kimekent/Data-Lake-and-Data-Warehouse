This repository contains the following files: 

Twitter Files: 

get_tweets_apache.py: Run by the tweets dag. This python script gets the tweets.

Add Sentiment, Write to Ware House.py: Run by the sentiment dag. This python script does conducts the duplicate check and sentiment analysis and writes the non duplicate tweets with their sentiment to the data warehouse. 

average.py: Run by the average dag. This python script creates an aggregated table by city  out of the tweets table in the data warehouse.

twitter_dag.py:  Script that runs the "get_tweets_apache.py" script.

sentiment_dagt.py: Script that runs the "Add Sentiment, Write to Ware House.py" script.

average_dag.py: Script that runs the "average.py" script.



Weather Files


Cost Files
