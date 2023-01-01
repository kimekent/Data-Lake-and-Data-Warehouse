import os
import psycopg2
import pandas as pd
import numpy as np
import boto3
import csv
import json
import cred

#Credentials to Data warehouse, environmental variables were added in Lambda function
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

#Connecting to S3 bucket
s3_client = boto3.client('s3')
S3_BUCKET_NAME = 'hslu-tests3tords' 

#Function to read csv file
def lambda_handler(event, context):
  object_key = "cost_living_prices_europe_cleanV2.csv"  # replace object key: File name in my case
  file_content = s3_client.get_object(
      Bucket=S3_BUCKET_NAME, Key=object_key)["Body"].read()
  print(file_content)
      
  testdb()
    
#Function to connect to DB and create table to insert data from s3 bucket
def testdb():
    
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    
    # Auto commit
    conn.set_session(autocommit=True)
    
    #Create Table in RDS if it doesn't exist
    # cur.execute("DROP TABLE IF EXISTS costlivingprices; CREATE TABLE costlivingprices \
    # (_id int, city_id int, city_name varchar(500), country_name varchar(500), good_id int, item_name varchar(500),\
    #     category_id int, category_name varchar(500), price_min int, price_avg int, price_max int, usd_min int, usd_avg int, usd_max int, \
    #         measure varchar(500), currency_code varchar(500));")
    
    #cur.execute("INSERT INTO costlivingprices (_id, city_id, city_name, country_name, good_id, item_name,\
    #    category_id, category_name, price_min, price_avg, price_max, usd_min, usd_avg, usd_max, \
    #    measure, currency_code) \
    #    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    # 
    # conn.commit()
    # ---------------------------------------------------------------------------------------
    # Checking whether it works, by executing a command  
    try:
        cur.execute("SELECT *FROM costlivingprices ORDER BY city_name LIMIT 10;")
    except psycopg2.Error as e:
        print("Error: select *")
        print (e)

    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()
    
    cur.close()
    conn.close()