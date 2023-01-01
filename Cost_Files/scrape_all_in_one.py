import requests
import csv
import json
import time
import cred         #credentials file

# read csv file
with open('european_data.csv', 'r', encoding='utf_8_sig') as f:
    reader = csv.reader(f)
    data = list(reader)

# set up API, get the API details on RapidAPI: Cost of living and prices -> Get Prices option
url = "https://cost-of-living-and-prices.p.rapidapi.com/prices"
headers = {
    "X-RapidAPI-Key": "SIGN-UP-FOR-KEY",
    "X-RapidAPI-Host": "cost-of-living-and-prices.p.rapidapi.com"
}

# set up counter
counter = 0

#open/create json file(PATH, File NAME.json, write, encoding(optional))
with open('C:/Users/Pepin/Documents/HS22/DLW/KLS/cost_of_living_scrape/scrape/'+'scrape_costlivingprice'+'.json', 'w', encoding='utf-8') as outfile:

# loop through csv file
    for row in data:
        # set up querystring
        querystring = {"city_name":row[0],"country_name":row[1]}
        # make request
        response = requests.request("GET", url, headers=headers, params=querystring)
        # save response in json file
        json.dump(response.json(), outfile, ensure_ascii=False, indent=4)
        
        # increment counter: Basic subscription allows only 10 requests per hour
        counter += 1
        if counter >= 9:        #while testing it out on RapidAPI only 9 requests were allowed
            # wait for 3600 second = 1 hour
            time.sleep(3600)
            counter = 0
        else:
            continue
        # print counter
        print(counter)