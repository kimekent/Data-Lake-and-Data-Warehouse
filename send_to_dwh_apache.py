#Connect to Datawarehouse and Datalake to transfer data from lake to warehouse

def load_dwh():

    try:
        conn = psycopg2.connect("host=kimstestdb.cujm2drdr40t.us-east-1.rds.amazonaws.com dbname=kimstestdb user=postgres password=315096KEK")

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres datalake")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Datalake")
        print(e)
    conn.set_session(autocommit=True)

#Connect to Database (RDS)
    try:
        conn2 = psycopg2.connect("host=sgdl1.cjf3sww93fr9.us-east-1.rds.amazonaws.com dbname=sgdl1 user=postgres password=1578SMDL")

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur2 = conn2.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
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


#create table in DWH with only the most important parameters (city, datetime, maxtemp, conditions)
    cur2.execute("CREATE TABLE IF NOT EXISTS SimpleWeatherForecast (city varchar(3000), date DATE,"
                "maxt numeric(30), mint numeric(30),"
               "conditions varchar(30), cloudcover numeric(30));")

   
#Print all rows in the table
    postgreSQL_select_Query = "SELECT * FROM weatherforecast"
    cur.execute(postgreSQL_select_Query)
    results = cur.fetchall()
    for row in results:
        print(row)
        print("-----------")
        city_string = row[0]
        print(city_string)
        date = row[1]
        print(date)
        maxTemp = row[2]
        print(maxTemp)
        minTemp = row[3]
        condition = row[6]
        print(condition)
        cloudcover = row[10]
        
#insert the parameters into the DataLake       
        try:
                cur2.execute("INSERT INTO SimpleWeatherForecast (city,date,maxt,mint,conditions,cloudcover) VALUES (%s,%s,%s,%s,%s,%s)",
                    (city_string,date,maxTemp,minTemp,condition,cloudcover))
                print("success")
        except:
                print("Error in insert")
                

        cur2.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")


        for table in cur2.fetchall():
            print("---Existing tables---")
            print(table)
        
    cur2.close()
    conn2.close()

    cur.close()
    conn.close()
