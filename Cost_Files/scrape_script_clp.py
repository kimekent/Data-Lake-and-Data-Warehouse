import requests
import csv
import json
import time
import cred     #my credentials file for API access

# read csv file
with open('european_data.csv', 'r') as f:
    reader = csv.reader(f)
    data = list(reader)

# set up API
url = "https://cost-of-living-and-prices.p.rapidapi.com/prices"
headers = {
    "X-RapidAPI-Key": "SIGN-UP-FOR-KEY",
    "X-RapidAPI-Host": "cost-of-living-and-prices.p.rapidapi.com"
}

# set up counter
counter = 0

# loop through csv file
for row in data:
    # set up querystring
    querystring = {"city_name":row[0],"country_name":row[1]}
    # make request
    response = requests.request("GET", url, headers=headers, params=querystring)
    # save response in json file
    with open('C:/Users/Pepin/Documents/HS22/DLW/KLS/cost_of_living_scrape/scrape/'+row[0]+'_'+row[1]+'.json', 'w') as outfile:
        json.dump(response.json(), outfile)
    
    # increment counter: API only allows 10 requests per hour
    counter += 1
    if counter >= 9:    #when testing it out only 9 are allowed
        # wait for 3600 second = 1 hour
        time.sleep(3600)
        counter = 0
    else:
        continue
    # print counter
    print(counter)