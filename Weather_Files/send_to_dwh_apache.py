#Connect to Datawarehouse and Datalake to transfer data from lake to warehouse

def load_dwh():
    
#Connect to Data Lake
    try:
        from credentials import dl_credentials
        
        conn = dl_credentials()
             
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres datalake")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Datalake")
        print(e)
    conn.set_session(autocommit=True)

#Connect to Data Warehouse
    try:
        
        from credentials import dwh_credentials
        
        conn2 = dwh_credentials()

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur2 = conn2.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Data Warehouse")
        print(e)

    conn2.set_session(autocommit=True)


#Delete existing table
    try:
        cur2 = conn2.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn2.set_session(autocommit=True)
    cur2.execute("DROP TABLE IF EXISTS SimpleWeatherForecast;""")


#create table in DWH with only the most important parameters (city, datetime, maxtemp, conditions, precipitation)
    cur2.execute("CREATE TABLE IF NOT EXISTS SimpleWeatherForecast (city varchar(3000), date DATE,"
                "maxt numeric(30), mint numeric(30),"
               "conditions varchar(30), cloudcover numeric(30), precipitation numeric(30));")


#Print some rows in the table
    postgreSQL_select_Query = "SELECT * FROM weatherforecast"
    cur.execute(postgreSQL_select_Query)
    results = cur.fetchall()
    for row in results:
        print("-----------")
        city_string = row[0]
        date = row[1]
        maxTemp = row[2]
        minTemp = row[3]
        condition = row[6]
        cloudcover = row[10]
        precipitation = row[11]
        print(city_string + " " + str(date))
        
#insert the parameters into the DataLake       
        try:
                cur2.execute("INSERT INTO SimpleWeatherForecast (city,date,maxt,mint,conditions,cloudcover,precipitation) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (city_string,date,maxTemp,minTemp,condition,cloudcover,precipitation))
                print("success")
        except:
                print("Error in insert")
                
        
    cur2.close()
    conn2.close()

    cur.close()
    conn.close()
