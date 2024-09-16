import psycopg2

import pandas as pd

import datetime

import uuid

# Database Connector which Connect with Postgresql
def postgres_connector_get(user_name , pass_word , query):
    connector = None
    cursor = None
    try:
        connector = psycopg2.connect(database='Final_Project' , user=user_name , password = pass_word)
        cursor = connector.cursor()

        cursor.execute(query)
        all_data = cursor.fetchall()
        return all_data

    except Exception as e:
        print(f"Someting Went Wrong will connectivity: {e}")
    finally:
        if connector != None and cursor != None:
            cursor.close()
            connector.close()



def postgres_connector_store(user_name , pass_word , query , data , bulk = False):
    connector = None
    cursor = None
    try:
        connector = psycopg2.connect(database='Final_Project' , user=user_name , password = pass_word)
        cursor = connector.cursor()

        if bulk is not False :
            cursor.executemany(query , data)
        else:
            cursor.execute(query)

        connector.commit()

    except Exception as e:
        print(f"Someting Went Wrong will connectivity: {e}")
    finally:
        if connector != None and cursor != None:
            cursor.close()
            connector.close()


def connector_for_pandas(user_name , pass_word , df , table_name):
    tuples = list(set([tuple(x) for x in df.to_numpy()]))
    
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO "
    query += table_name
    query += "(job_id , title ,  company , posted_on , date_of_extract , salary , location , description , link , user_id , url_name)" 
    query +=  "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" 
    
    #print(tuples[-3])

    cursor = None
    try:
        connector = psycopg2.connect(database='Final_Project' , user=user_name , password = pass_word)
        cursor = connector.cursor()

        cursor.executemany(query , tuples)
        connector.commit()
        return True

    except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            connector.rollback()
            return False

    finally:
        if connector != None and cursor != None:
            cursor.close()
            connector.close()
    

if __name__ == "__main__":
    q = 'SELECT * FROM urls;'

    # print("user data : " , postgres_connector_get('postgres' , '1407' , "SELECT * FROM user_data;"))
    
    # print("urls data : " , postgres_connector_get('postgres' , '1407' , "SELECT * FROM urls;"))
    
    # print("ETL data : " , postgres_connector_get('postgres' , '1407' , "SELECT * FROM etl_data;"))
    
    # print("user url data : " , postgres_connector_get('postgres' , '1407' , "SELECT * FROM user_url_info;"))

    # q = '''insert INTO job_data (job_id , title , company , posted_on , "date _of_extract" , salary , "location" , description , link , user_id) values( \''''
    # q += str(uuid.uuid4())
    # q += '''\','saim' , 'ahmed' , '2020-03-12' , '2020-03-12' , '30L' , 'Bhopal' , 'None' , 'None' , 2) ;'''

    # postgres_connector_store('postgres' , '1407' , q , "NOne")

    

    
    data = {
            'job_id' : ['765f5d9b-6971-4663-97b3-a8755bddc21b', 'ac9f8513-a48d-4085-8b84-5da6ec442124', '0b0d0002-f019-45be-87fa-de2cd76281b4', 'bdc7bb3b-aef5-4f09-a903-38d71138dab4', '0e6d3e46-ba55-452e-b7a9-af56512ed124', 'def79ae3-be7f-4615-a9bf-4bdb0c4f40e8', '4d9525d4-ae14-4d4f-8c47-4ae1b3259159', '6a952c30-28f9-4b5b-96e0-c79dbef6e5d6', '58fb012c-fbaa-41c9-80a1-6451a022405f', 'bcf7826c-01df-4e03-83d8-d4e043374909', '0b3333f3-1f09-428e-8402-ea95758da12a', 'cc76004e-203f-4973-a2d9-8686dc2b379b', '59fdec85-bfdf-481a-aaf2-31786a5aa3bd', 'bc3acfbc-6c2c-451d-9467-13556de2760f', 'ca7b6041-5ddc-4076-a1f4-fcff0b45e387', '92633607-c94f-4e35-8624-2c98d299d442', '67482e54-c70b-4d05-a48f-750a284c0bc7', '4467c943-c664-4e51-8898-cf9e57d28b52', 'c6a4f1e5-cf72-4202-aed1-8864846e9a94', '850d8ddb-be7a-484a-aa50-3137b26ed0c6', '9e6e656e-8422-4643-8bda-0a60cc01393f', '1a15f339-67a7-4fd0-95c7-08035f2a1574', '39939417-e370-4875-a29c-f6985876c6c5', '7bfc811f-564c-4331-9d32-a2c5a5c56b46', 'b73a457e-4be5-4c32-83a4-4b80a683f7bc', '48f45030-4d8a-4843-af98-f71b14569361', '09f2de7b-c7a1-4a38-bd86-86c0cf59276e', '8120e9cb-8ef1-4d8c-87a6-174fb0a057ea', '36c292f1-b7db-42ff-b474-ac8873bc789c', '9631427a-2a94-43c7-b311-b7f6da4d87e5', '80fcef06-6420-4988-b3f7-8d76f1aed685', 'd16114ad-386a-47e9-a455-466888242c70', '8f9695e1-2cbd-4f17-a1e3-4b19f0344817', 'af450853-982d-4231-9e74-d7e4f54d2c9e'],
            'title': ['Misroad', 'Kolar Road', 'Ayodhya Bypass Road', 'Awadhpuri', 'Misrod', 'Baghmugalia', 'Hoshangabad', 'Bagh Swaniya', 'Kolar Road', 'Kolar Road', 'Barkheri', 'Shivlok Phase 3', 'Hoshangabad Road', 'Awadhpuri', 'Arera Colony', 'Gulmohar Colony', 'Ayodhya Nagar', 'Shahjahanabad', 'Ayodhya Bypass Road', 'Katara Hills', 'Katara Hills', 'Kolar Road', 'Chinarr Dream CT', 'Kotra Sultanabad', 'Kolar Road', 'Mandideep', 'Baghmugalia', 'Maa Vaishnav Sunrise Heights', 'Karond', 'Bangrasia', 'Nariyalkheda', 'Agrawal Sagar Premium Towers', 'Airport Road', 'Bhojpur Road'],
            'company': ['1250 Sq.Yd.\nPlot Area', '2400 Sq.Ft.\nPlot Area', '1800 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '800 Sq.Ft.\nPlot Area', '450 Sq.Ft.\nPlot Area', '646 Sq.Ft.\nPlot Area', '1225 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '3000 Sq.Ft.\nPlot Area', '1815 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '800 Sq.Ft.\nPlot Area\nReady To Move\nPossession Status', '4000 Sq.Ft.\nPlot Area', '752 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '7000 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '600 Sq.Ft.\nPlot Area', '475 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '1042 Sq.Ft.\nSaleable Area\nReady To Move\nPossession Status', '2000 Sq.Ft.\nPlot Area', '618 Sq.Ft.\nCarpet Area\nReady To Move\nPossession Status', '948 Sq.Ft.\nBuilt-up Area\nUnder Construction\nPossession Status', '752 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '1750 Sq.Ft.\nPlot Area\nReady To Move\nPossession Status', '945 Sq.Ft.\nPlot Area\nReady To Move\nPossession Status', '800 Sq.Ft.\nSaleable Area\nReady To Move\nPossession Status', '1000 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '1500 Sq.Yd.\nPlot Area', '1 Acre\nPlot Area', '1350 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '1800 Sq.Ft.\nBuilt-up Area\nUnder Construction\nPossession Status', '223 Sq.Ft.\nBuilt-up Area\nUnder Construction\nPossession Status', '880 Sq.Ft.\nPlot Area', '7200 Sq.Ft.\nPlot Area', '1150 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '1250 Sq.Ft.\nBuilt-up Area\nReady To Move\nPossession Status', '2 Acre\nPlot Area'], 
            'posted_on': [datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20)], 
            'date_of_extract': [datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20), datetime.date(2023, 4, 20)], 
            'salary': ['₹ 28.75 L', '₹ 1.25 Cr.', '₹ 82 L', '₹ 17.6 L', '₹ 5 L', '₹ 16.5 L', '₹ 39.9 L', '₹ 99 L', '₹ 65.9 L', '₹ 35 L', '₹ 30 L', '₹ 21 L', '₹ 4.75 Cr.', '₹ 13.2 L', '₹ 16.75 L', '₹ 23 L', '₹ 20 L', '₹ 23 L', '₹ 24.99 L', '₹ 31 L', '₹ 1 Cr.', '₹ 35 L', '₹ 19 L', '₹ 72 L', '₹ 30 L', '₹ 1.85 Cr.', '₹ 35 L', '₹ 49.99 L', '₹ 23.63 L', '₹ 15 L', '₹ 2.16 Cr.', '₹ 32 L', '₹ 38 L', '₹ 80 L'], 
            'location': ['Misroad\nPlot Misroad', 'Kolar Road\nPlot Kolar Road', 'Ayodhya Bypass Road\n2 BHK Independent House Ayodhya Bypass Road', 'Awadhpuri\nPlot Awadhpuri', 'Misrod\nPlot Misrod', 'Baghmugalia\nPlot Baghmugalia', 'Hoshangabad\n2 BHK Flat Hoshangabad', 'Bagh Swaniya\nPlot Bagh Swaniya', 'Kolar Road\n4 BHK Flat Kolar Road', 'Kolar Road\n4 BHK Independent House Kolar Road', 'Barkheri\nPlot Barkheri', 'Shivlok Phase 3\n2 BHK Flat Shivlok Phase 3', 'Hoshangabad Road\n5 BHK Villa Hoshangabad Road', 'Awadhpuri\nPlot Awadhpuri', 'Arera Colony\n1 BHK Flat Arera Colony', 'Gulmohar Colony\n2 BHK Flat Gulmohar Colony', 'Ayodhya Nagar\nPlot Ayodhya Nagar', 'Shahjahanabad\n2 BHK Flat Shahjahanabad', 'Ayodhya Bypass Road\n2 BHK Flat Ayodhya Bypass Road', 'Katara Hills\n2 BHK Flat Katara Hills', 'Katara Hills\n4 BHK Villa Katara Hills', 'Kolar Road\nWarehouse Kolar Road', 'Chinarr Dream CT\n2 BHK Flat Ratanpur', 'Kotra Sultanabad\n4 BHK Flat Kotra Sultanabad', 'Kolar Road\nPlot Kolar Road', 'Mandideep\nIndustrial Plot Mandideep', 'Baghmugalia\n3 BHK Flat Baghmugalia', 'Maa Vaishnav Sunrise Heights\n4 BHK Flat Hoshangabad', 'Karond\nShop Karond', 'Bangrasia\nPlot Bangrasia', 'Nariyalkheda\nPlot Nariyalkheda', 'Agrawal Sagar Premium Towers\n3 BHK Flat Kolar Road', 'Airport Road\n4 BHK Villa Airport Road', 'Bhojpur Road\nLand Bhojpur Road'], 
            'description': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None], 
            'link': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
            'user_id' : [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            'url_name' : ['squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com', 'squareyards.com']
            }
    df = pd.DataFrame(data=data , columns=['job_id' , 'title' , 'company' , 'posted_on' , 'date_of_extract' , 'salary' , 'location' , 'description' , 'link' , 'user_id' , 'url_name'])
    #df['etl_id'] = 2
    #print(df)

    conn = connector_for_pandas('postgres' , '1407' , df , 'job_data')
    print(conn)
    
    
    