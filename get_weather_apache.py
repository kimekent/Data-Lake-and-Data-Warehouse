#Extract Weather Data from API, Connect to Datalake and insert into table

#Needed imports
import requests
import psycopg2
import json


def get_weather():

    #Connect to Datalake (RDS)
    try:
        conn = psycopg2.connect("host=kimstestdb.cujm2drdr40t.us-east-1.rds.amazonaws.com dbname=kimstestdb user=postgres password=315096KEK")

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Datalake")
        print(e)

    conn.set_session(autocommit=True)

#check exising tables in data lake
    cur.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        print(table)

#Delete existing weather table
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn.set_session(autocommit=True)
    cur.execute("DROP TABLE IF EXISTS WeatherForecast;""")


#Create new table weatherforecast
    cur.execute("CREATE TABLE IF NOT EXISTS WeatherForecast (address varchar(3000), date DATE,"
                "maxt numeric(30), mint numeric(30), temp numeric(30),    "
               "humidity numeric(30), conditions varchar(30), wdir numeric(100), windspeed numeric(30),"
                "windchill numeric(30), cloudcover numeric(30), precipitation numeric(30) );")

#Get names of cities we want to find weather data for in the table capitals
    city_name = []
    sql = """ select capital from capitals; """
    cur.execute(sql)
    for i in cur.fetchall():
        i = i[0]
        city_name.append(i)
    print(city_name)

   
#connect to API and iterate through city_names
#Credentials
    host = "visual-crossing-weather.p.rapidapi.com"
    key = "b456ae568dmshc70618f5fee40d1p16fef3jsn076b637574a9" #this doesn't occur in Airflow Webbrowser


    for city in city_name:
            url = "https://visual-crossing-weather.p.rapidapi.com/forecast"
            querystring = {"aggregateHours":"24","location": city,"unitGroup":"metric","contentType":"json","shortColumnNames":"true", "lang":"en"}
            headers = {
            "X-RapidAPI-Key": key,
            "X-RapidAPI-Host": host
            }

            try:
                response = requests.request("GET", url, headers=headers, params=querystring)
                json_response = response.json()

            except:
                print("Bad request")
                pass

#match the needed parameters with the keys and values from the json_response for every city (issues: Riga-> Riga, Latvian; Kiev -> Kyiv)
            for i in range(len(json_response)):
                if city == 'Riga':
                    city == 'Riga, Latvian'

                elif city == 'Kiev':
                    city == 'Kyiv'

                else:
                    city == city

                    address = list(json_response['locations'].keys())
                    address = address[0] #becuase list isn't hashable
                    date = json_response['locations'][city].get('values')[i].get('datetimeStr')
                    maxTemp = json_response['locations'][city].get('values')[i].get('maxt')
                    minTemp = json_response['locations'][city].get('values')[i].get('mint')
                    meanTemp = json_response['locations'][city].get('values')[i].get('temp')
                    humidity = json_response['locations'][city].get('values')[i].get('humidity')
                    condition = json_response['locations'][city].get('values')[i].get('conditions')
                    winddirection =json_response['locations'][city].get('values')[i].get('wdir')
                    windspeed = json_response['locations'][city].get('values')[i].get('wspd')
                    windchill = json_response['locations'][city].get('values')[i].get('windchill')
                    cloudcover = json_response['locations'][city].get('values')[i].get('cloudcover')
                    precipitation = json_response['locations'][city].get('values')[i].get('precip')

                    #printing most important results
                    print('date:'+ date)
                    print('city:' + str(address))
                    print("Max Temperature: " + str(maxTemp))
                    print('Condition: '+ str(condition))

                #insert the parameters into the DataLake
                try:
                    cur.execute("INSERT INTO WeatherForecast (address,date,maxt,mint,temp,humidity,conditions,wdir, windspeed, windchill, cloudcover, precipitation) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (address,date,maxTemp,minTemp,meanTemp,humidity,condition,winddirection,windspeed,windchill, cloudcover, precipitation))
                    print("success")
                except:
                    print("Error in insert")

#check wether data was inserted into table in DataLake
    sql_check = """select * from WeatherForecast;"""
    cur.execute(sql_check)
    results = cur.fetchall()

    cur.close()
    conn.close()
