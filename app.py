from mysql.connector import pooling
import os
import json
import pandas as pd
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import requests

st.set_page_config(
    page_title="PhonePe Pulse",
    page_icon="📊",
    layout="wide",
)

with st.sidebar:
    selected = option_menu(
        menu_title = "",
        options = ["HOME","EXPLORE DATA","INSIGHTS","REPORTS"],
        icons=["house","bar-chart","bar-chart-fill","bar-chart-steps"],
        default_index = 0,
        
        styles={
        "container": {"padding": "0!important", "background-color": "#EBEDEF"},
        "icon": {"color": "orange", "font-size": "20px"},
        "nav-link": {"--hover-color": "#958fe9", "color": "#080354", "font-size": "20px"},
        "nav-link-selected": {"background-color": "#7ef5ea"},
    }
        
    )
    
st.markdown("""
    <style>
        .header {
            font-size:40px;
            font-weight:bold;
            color: white;
            background-color: #3c115f;
            padding: 10px;
            text-align: center;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            z-index: 9999;
        }
        .main {
            margin-top: 80px; /* Adjust this based on your header height */
        }
        
        /* Hide Streamlit footer and menu */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
    </style>
    <div class="header">
        Phonepe Pulse Data Visualization and Exploration
    </div>
""", unsafe_allow_html=True)
    



# Aggregated Insurance data
def aggregate_insurance(Aggrgate_insurance_path):
    Aggrgate_insurance_data = {"State": [], "Year": [], "Quarter": [], "Insurance_type": [],  "Insurance_Count": [], "Insurance_Amount": []}
    for state in os.listdir(Aggrgate_insurance_path):
        state_path = os.path.join(Aggrgate_insurance_path, state)
        if os.path.isdir(state_path):
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    for file_name in os.listdir(year_path):
                        if file_name.endswith('.json'):
                            file_path = os.path.join(year_path, file_name)
                            with open(file_path, 'r') as file:
                                data = json.load(file)
                                
                                for item in data.get('data', {}).get('transactionData', []):
                                    name = item['name']
                                    count = item['paymentInstruments'][0]['count']
                                    amount = item['paymentInstruments'][0]['amount']
                                    quarter = int(file_name.rstrip('.json'))
                                    
                                    Aggrgate_insurance_data["State"].append(state)
                                    Aggrgate_insurance_data["Year"].append(year)
                                    Aggrgate_insurance_data["Quarter"].append(quarter)
                                    Aggrgate_insurance_data["Insurance_type"].append(name)
                                    Aggrgate_insurance_data["Insurance_Count"].append(count)
                                    Aggrgate_insurance_data["Insurance_Amount"].append(amount)

    return Aggrgate_insurance_data
    
Aggregate_insurance_data = aggregate_insurance("D:/01_Guvi project/PhonePe Pulse/pulse/data/aggregated/insurance/country/india/state/")

def aggregate_insurance_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['Insurance_Amount'] = df['Insurance_Amount'].astype('int64')
    return df

aggregate_insurance_clean_data = aggregate_insurance_cleaning(Aggregate_insurance_data)

# Aggregated Transaction data
def aggregate_transaction(Aggregate_transaction_path):
    Aggrgate_transaction_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Transaction_type": [],
        "Transaction_Count": [],
        "Transaction_Amount": []
    }

    for state in os.listdir(Aggregate_transaction_path):
        state_path = os.path.join(Aggregate_transaction_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        
                        for item in data.get('data', {}).get('transactionData', []):
                            name = item['name']
                            count = item['paymentInstruments'][0]['count']
                            amount = item['paymentInstruments'][0]['amount']
                            quarter = int(file_name.rstrip('.json'))
                            
                            Aggrgate_transaction_data["State"].append(state)
                            Aggrgate_transaction_data["Year"].append(year)
                            Aggrgate_transaction_data["Quarter"].append(quarter)
                            Aggrgate_transaction_data["Transaction_type"].append(name)
                            Aggrgate_transaction_data["Transaction_Count"].append(count)
                            Aggrgate_transaction_data["Transaction_Amount"].append(amount)

    return Aggrgate_transaction_data
           
Aggregate_transaction = aggregate_transaction("D:/01_Guvi project/PhonePe Pulse/pulse/data/aggregated/transaction/country/india/state/")

# Aggregated Transaction data cleaning
def aggregate_transaction_clean(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['Transaction_Amount'] = df['Transaction_Amount'].astype('int64')
    return df

aggregate_transaction_clean_data = aggregate_transaction_clean(Aggregate_transaction)

# Aggregated user data
def aggregate_user(Aggregate_user_path):

    Aggregate_user_data = {
        "States": [],
        "Years": [],
        "Quarter": [],
        "User_device": [],
        "User_count": [],
        "User_percentage": []
    }

    for state in os.listdir(Aggregate_user_path):
        state_path = os.path.join(Aggregate_user_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        user_by_device = data.get('data', {}).get('usersByDevice', [])
                        if user_by_device:
                            for item in user_by_device:
                                #print(item[0])
                                try:
                                    brand = item['brand']
                                    count = item['count']
                                    percentage = item['percentage']
                                    quarter = int(file_name.rstrip('.json'))
                                    Aggregate_user_data['States'].append(state)
                                    Aggregate_user_data['Years'].append(year)
                                    Aggregate_user_data['Quarter'].append(quarter)
                                    Aggregate_user_data['User_device'].append(brand)
                                    Aggregate_user_data['User_count'].append(count)
                                    Aggregate_user_data['User_percentage'].append(percentage)
                                except Exception as e:
                                    continue
    return Aggregate_user_data
                                   
Aggregate_user_path = aggregate_user("D:/01_Guvi project/PhonePe Pulse/pulse/data/aggregated/user/country/india/state/")

# Aggregated user data cleaning
def aggregate_user_cleaning(df):
    df = pd.DataFrame(df)
    df['States'] = df['States'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['User_percentage'] = df['User_percentage'].apply(lambda x: round(x*100, 2))
    return df

aggregate_user_clean_data = aggregate_user_cleaning(Aggregate_user_path)

# Map Insurance data
def  map_insurance(map_insurance_path):
    #map_insurance_path = "D:/01_Guvi project/PhonePe Pulse/pulse/data/map/insurance/hover/country/india/state/"
    map_insurance_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Insurance_district": [],
        "Insurance_Count": [],
        "Insurance_Amount": []
    }

    for state in os.listdir(map_insurance_path):
        state_path = os.path.join(map_insurance_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        #print(data)
                        
                        for item in data.get('data', {}).get('hoverDataList', []):
                            name = item['name']
                            count = item['metric'][0]['count']
                            amount = item['metric'][0]['amount']
                            quarter = int(file_name.rstrip('.json'))
                            
                            map_insurance_data["State"].append(state)
                            map_insurance_data["Year"].append(year)
                            map_insurance_data["Quarter"].append(quarter)
                            map_insurance_data["Insurance_district"].append(name)
                            map_insurance_data["Insurance_Count"].append(count)
                            map_insurance_data["Insurance_Amount"].append(amount)
    
    return map_insurance_data                  

Map_insurance_path = map_insurance("D:/01_Guvi project/PhonePe Pulse/pulse/data/map/insurance/hover/country/india/state/")

# Map Insurance data cleaning
def map_insurance_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['Insurance_district'] = df['Insurance_district'].str.title()
    return df

Map_insurance_clean_data = map_insurance_cleaning(Map_insurance_path) 

# Map transaction data
def map_transaction(map_transaction_path):
    map_transaction_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Transaction_district": [],
        "Transaction_Count": [],
        "Transaction_Amount": []
    }

    for state in os.listdir(map_transaction_path):
        state_path = os.path.join(map_transaction_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        #print(data)
                        
                        for item in data.get('data', {}).get('hoverDataList', []):
                            name = item['name']
                            count = item['metric'][0]['count']
                            amount = item['metric'][0]['amount']
                            quarter = int(file_name.rstrip('.json'))
                            
                            map_transaction_data["State"].append(state)
                            map_transaction_data["Year"].append(year)
                            map_transaction_data["Quarter"].append(quarter)
                            map_transaction_data["Transaction_district"].append(name)
                            map_transaction_data["Transaction_Count"].append(count)
                            map_transaction_data["Transaction_Amount"].append(amount)

    return map_transaction_data

Map_transaction_path = map_transaction("D:/01_Guvi project/PhonePe Pulse/pulse/data/map/transaction/hover/country/india/state/")

# Map transaction data cleaning
def map_transaction_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['Transaction_district'] = df['Transaction_district'].str.title()
    df['Transaction_Amount'] = df['Transaction_Amount'].astype('int64')
    return df

Map_transaction_clean_data = map_transaction_cleaning(Map_transaction_path)               

# Map user data
def map_user(map_user_path):
    map_user_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "User_district": [],
        "User_registration": [],
        "User_appOpen": []
    }

    for state in os.listdir(map_user_path):
        state_path = os.path.join(map_user_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        #print(data)
                        
                        for district, value in data.get('data', {}).get('hoverData', {}).items():
                            register_user = value['registeredUsers']
                            app_open = value['appOpens']
                            quarter = int(file_name.rstrip('.json'))
                            
                            map_user_data["State"].append(state)
                            map_user_data["Year"].append(year)
                            map_user_data["Quarter"].append(quarter)
                            map_user_data["User_district"].append(district)
                            map_user_data["User_registration"].append(register_user)
                            map_user_data["User_appOpen"].append(app_open)
    return map_user_data                        
                           
Map_user_path = map_user("D:/01_Guvi project/PhonePe Pulse/pulse/data/map/user/hover/country/india/state/")

# Map user data cleaning
def map_user_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['User_district'] = df['User_district'].str.title()
    return df

Map_user_clean_data = map_user_cleaning(Map_user_path)

# Top Insurance data
def top_insurance(top_insurance_path):
    top_insurance_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Insurance_pincode": [],
        "Insurance_claims": [],
        "Insurance_amount": []
    }

    for state in os.listdir(top_insurance_path):
        state_path = os.path.join(top_insurance_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        #print(data)
                        
                        for item in data.get('data', {}).get('pincodes', []):
                            name = item['entityName']
                            count = item['metric']['count']
                            amount = item['metric']['amount']
                            quarter = int(file_name.rstrip('.json'))
                            
                            top_insurance_data["State"].append(state)
                            top_insurance_data["Year"].append(year)
                            top_insurance_data["Quarter"].append(quarter)
                            top_insurance_data["Insurance_pincode"].append(name)
                            top_insurance_data["Insurance_claims"].append(count)
                            top_insurance_data["Insurance_amount"].append(amount)

    return top_insurance_data

Top_insurance_path = top_insurance("D:/01_Guvi project/PhonePe Pulse/pulse/data/top/insurance/country/india/state/")

# Top insurance cleaning
def top_insurance_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    return df

Top_insurance_clean_data = top_insurance_cleaning(Top_insurance_path)

# Top transation data
def top_transaction(top_transaction_path):
    top_transaction_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "Transaction_pincode": [],
        "Transaction_count": [],
        "Transaction_amount": []
    }

    for state in os.listdir(top_transaction_path):
        state_path = os.path.join(top_transaction_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        #print(data)
                        
                        for item in data.get('data', {}).get('pincodes', []):
                            name = item['entityName']
                            count = item['metric']['count']
                            amount = item['metric']['amount']

                            quarter = int(file_name.rstrip('.json'))
                            
                            top_transaction_data["State"].append(state)
                            top_transaction_data["Year"].append(year)
                            top_transaction_data["Quarter"].append(quarter)
                            top_transaction_data["Transaction_pincode"].append(name)
                            top_transaction_data["Transaction_count"].append(count)
                            top_transaction_data["Transaction_amount"].append(amount)

    return top_transaction_data

Top_transaction_path = top_transaction("D:/01_Guvi project/PhonePe Pulse/pulse/data/top/transaction/country/india/state/")
# Top transaction cleaning
def top_transaction_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['Transaction_amount'] = df['Transaction_amount'].astype('int64')
    return df

Top_transaction_clean_data = top_transaction_cleaning(Top_transaction_path)

# Top user data
def top_user(top_user_path):
    top_user_data = {
        "State": [],
        "Year": [],
        "Quarter": [],
        "User_pincode": [],
        "User_count": []
    }

    for state in os.listdir(top_user_path):
        state_path = os.path.join(top_user_path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file_name in os.listdir(year_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(year_path, file_name)
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        #print(data)
                        
                        for item in data.get('data', {}).get('pincodes', []):
                            name = item['name']
                            count = item['registeredUsers']

                            quarter = int(file_name.rstrip('.json'))
                            
                            top_user_data["State"].append(state)
                            top_user_data["Year"].append(year)
                            top_user_data["Quarter"].append(quarter)
                            top_user_data["User_pincode"].append(name)
                            top_user_data["User_count"].append(count)

    return top_user_data

Top_user_path = top_user("D:/01_Guvi project/PhonePe Pulse/pulse/data/top/user/country/india/state/")

# Top user cleaning
def top_user_cleaning(df):
    df = pd.DataFrame(df)
    df['State'] = df['State'].str.title().str.replace('-', ' ').str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu').str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df['User_count'] = df['User_count'].astype('int64')
    return df
Top_user_clean_data = top_user_cleaning(Top_user_path)

def connection():
    config = {
        'user': 'root',
        'password':'Your_password',
        'host': 'localhost',
        'auth_plugin': 'mysql_native_password',
        'database': 'Your_database_Name',
    }
    try:
        pool = pooling.MySQLConnectionPool(pool_name="mypool",
                                           pool_size=5,
                                           **config)
        conn = pool.get_connection()
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error connecting to MySQL: {err}")
        return None
          
def insert_aggregate_insurance_data_to_mysql(data,conn):
    
    try:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS aggregate_insurance')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS aggregate_insurance (
                State VARCHAR(75),
                Year INT,
                Quarter INT,
                Insurance_type VARCHAR(50),
                Insurance_count INT,
                Insurance_amount BIGINT
            
            );   
        ''')
        
        insert_query = '''
        INSERT INTO aggregate_insurance (State, Year, Quarter, Insurance_type, Insurance_count, Insurance_amount) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Insurance_type = VALUES(Insurance_type),
        Insurance_count = VALUES(Insurance_count),
        Insurance_amount = VALUES(Insurance_amount);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Aggregate Insurance")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
        
def insert_aggregate_transaction_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS aggregate_transaction')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS aggregate_transaction (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                Transaction_type VARCHAR(255),
                Transaction_count BIGINT,
                Transaction_amount BIGINT
            
            );   
        ''')
        
        insert_query = '''
        INSERT INTO aggregate_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Transaction_type = VALUES(Transaction_type),
        Transaction_count = VALUES(Transaction_count),
        Transaction_amount = VALUES(Transaction_amount);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Aggregate Transaction")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
        
def insert_aggregate_user_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS aggregate_user')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS aggregate_user (
                State VARCHAR(60),
                Year INT,
                Quarter INT,
                User_Device VARCHAR(50),
                User_count INT,
                User_percentage FLOAT
            
            );   
        ''')
        
        insert_query = '''
        INSERT INTO aggregate_user (State, Year, Quarter, User_Device, User_count, User_percentage) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        User_Device = VALUES(User_Device),
        User_count = VALUES(User_count),
        User_percentage = VALUES(User_percentage);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Aggregate User")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
        
def insert_map_insurance_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS map_insurance')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS map_insurance (
                State VARCHAR(75),
                Year INT,
                Quarter INT,
                Insurance_district VARCHAR(50),
                Insurance_Count INT,
                Insurance_Amount BIGINT
            
            );   
        ''')
        insert_query = '''
        INSERT INTO map_insurance (State, Year, Quarter, Insurance_district, Insurance_Count, Insurance_Amount) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Insurance_district = VALUES(Insurance_district),
        Insurance_Count = VALUES(Insurance_Count),
        Insurance_Amount = VALUES(Insurance_Amount);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Map Insurance")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
        
def insert_map_transaction_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS map_transaction;")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS map_transaction (
                State VARCHAR(50),
                Year INT,
                Quarter INT,
                Transaction_district VARCHAR(50),
                Transaction_Count INT,
                Transaction_Amount BIGINT
            
            );   
        ''')
        insert_query = '''
        INSERT INTO map_transaction (State, Year, Quarter, Transaction_district, Transaction_Count, Transaction_Amount) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Transaction_district = VALUES(Transaction_district),
        Transaction_Count = VALUES(Transaction_Count),
        Transaction_Amount = VALUES(Transaction_Amount);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Map Transaction")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
def insert_map_user_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS map_user;")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS map_user (
                State VARCHAR(60),
                Year INT,
                Quarter INT,
                User_district VARCHAR(50),
                User_registration INT,
                User_appOpen INT
            
            );   
        ''')
        insert_query = '''
        INSERT INTO map_user (State, Year, Quarter, User_district, User_registration, User_appOpen) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        User_district = VALUES(User_district),
        User_registration = VALUES(User_registration),
        User_appOpen = VALUES(User_appOpen);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Map User")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()

def insert_top_insurance_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS top_insurance;")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS top_insurance (
                State VARCHAR(75),
                Year INT,
                Quarter INT,
                Insurance_pincode VARCHAR(50),
                Insurance_claims INT,
                Insurance_amount BIGINT
            
            );   
        ''')
        insert_query = '''
        INSERT INTO top_insurance (State, Year, Quarter, Insurance_pincode, Insurance_claims, Insurance_amount) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Insurance_pincode = VALUES(Insurance_pincode),
        Insurance_claims = VALUES(Insurance_claims),
        Insurance_amount = VALUES(Insurance_amount);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Top Insurance")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()

def insert_top_transaction_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS top_transaction;")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS top_transaction (
                State VARCHAR(50),
                Year INT,
                Quarter INT,
                Transaction_pincode VARCHAR(50),
                Transaction_Count INT,
                Transaction_Amount BIGINT
            
            );   
        ''')
        insert_query = '''
        INSERT INTO top_transaction (State, Year, Quarter, Transaction_pincode, Transaction_Count, Transaction_Amount) 
        VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        Transaction_pincode = VALUES(Transaction_pincode),
        Transaction_Count = VALUES(Transaction_Count),
        Transaction_Amount = VALUES(Transaction_Amount);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Top Transaction")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
        
def insert_top_user_data_to_mysql(data,conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS top_user;")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS top_user (
                State VARCHAR(60),
                Year INT,
                Quarter INT,
                User_pincode VARCHAR(50),
                User_count INT
           
            );   
        ''')
        insert_query = '''
        INSERT INTO top_user (State, Year, Quarter, User_pincode, User_count) 
        VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
        State = VALUES(State),
        Year = VALUES(Year),
        Quarter = VALUES(Quarter),
        User_pincode = VALUES(User_pincode),
        User_count = VALUES(User_count);
        '''
        data_tuples = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        st.success("Data inserted successfully into Top User")
    except Exception as e:
        st.error(f"Error inserting data: {str(e)}")
        
    finally:
        cursor.close()
def sql_query_executor(query):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Your_password",
            database="Your_database_Name"
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
            #st.success("Query executed successfully")
            return df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def refresh_button():
    
    data_to_insert = [
        (aggregate_insurance_clean_data,insert_aggregate_insurance_data_to_mysql),
        (aggregate_transaction_clean_data,insert_aggregate_transaction_data_to_mysql),
        (aggregate_user_clean_data,insert_aggregate_user_data_to_mysql),
        (Map_insurance_clean_data,insert_map_insurance_data_to_mysql),
        (Map_transaction_clean_data,insert_map_transaction_data_to_mysql),
        (Map_user_clean_data,insert_map_user_data_to_mysql),
        (Top_insurance_clean_data,insert_top_insurance_data_to_mysql),
        (Top_transaction_clean_data,insert_top_transaction_data_to_mysql),
        (Top_user_clean_data,insert_top_user_data_to_mysql)
    ]
    conn = connection()
    if not conn:
        return
    try:
        for data, insert_function in data_to_insert:
            insert_function(data,conn)
        st.markdown("""
            <style>
            .completed-message {
                font-size: 24px;
                font-weight: bold;
                color: #4CAF50; /* Green color */
                text-align: center;
                margin-top: 20px;
            }
            </style>
            <div class="completed-message">All data inserted successfully. Completed!</div>
            """, unsafe_allow_html=True)
    finally:
        conn.close()

def show_readme():
    st.title("📊 PhonePe Pulse Data Analysis and Visualization")
    
    st.markdown("""
    ## 🔍 **Project Overview**
    This project analyzes and visualizes PhonePe Pulse data using **Streamlit**, **Plotly**, and **MySQL**. 
    Explore trends in transactions, insurance, and user growth with interactive charts and maps.
    
    ## 🚀 **Features**
    - **Choropleth Maps**: Visualize state-level aggregates for transactions, users, and insurance.
    - **Box Plots**: Analyze distribution and outliers in insurance and transaction data.
    - **Dynamic Data Management**: Fetch and load data directly into MySQL.
    - **Interactive UI**: User-friendly Streamlit interface for selecting years, quarters, and data types.

    ## 🛠️ **Technologies Used**
    - **Python**, **Streamlit**, **MySQL**, **Plotly**
    - **Pandas** for data cleaning and manipulation
    - **MySQL Connector** with connection pooling

    ## ⚙️ **Setup Instructions**
    1. Clone the repository:
       ```bash
       git clone <your-repository-link>
       cd PhonePe_Pulse_Project
       ```
    2. Install dependencies:
       ```bash
       pip install -r requirements.txt
       ```
    3. Configure MySQL connection in the `connection()` function.
    4. Run the Streamlit app:
       ```bash
       streamlit run app.py
       ```

    ## 📈 **Insights**
    - Identify states with the highest transactions or insurance amounts.
    - Explore trends across quarters and years.
    - Detect outliers in transaction patterns.

    ## 📝 **Future Enhancements**
    - Add PDF and PPT export features for visualizations.
    - Integrate user authentication for data security.
    - Enable real-time data updates.

    ---
    """)    
if selected == 'HOME':
    st.markdown("""
    <style>
    .stButton button {
        position: fixed;
        top: 15px;
        right: 20px;
        background-color: #6a1b9a;    
        color: white;
        border: none;
        padding: 10px 20px;
        font-size: 16px;
        font-weight: bold;
        cursor: pointer;
        border-radius: 5px;
        z-index: 10000;
    }
    .stButton button:hover {
        background-color: #7e57c2;
    }
    </style>
    """, unsafe_allow_html=True)
    
    if st.button("Refresh"):
        refresh_button() 
    show_readme()

@st.cache_data
def fetch_geojson(url):
    response = requests.get(url)
    return json.loads(response.content)
    
@st.cache_data
def excute_query(query):
    return sql_query_executor(query)

if selected == 'EXPLORE DATA':
    col1, col2, col3 = st.columns(3)
    with col1:                   
        quarter = st.selectbox(
            'Select Quarter',
            ['Q1 (Jan - Mar)','Q2 (Apr - Jun)','Q3 (Jul - Sep)','Q4 (Oct - Dec)'])
    with col2:
        Year = st.selectbox(
            'Select Year',
            ['2018','2019','2020','2021','2022','2023','2024'])
    with col3:
        data_type = st.selectbox(
            'Select an option',
            ['INSURANCE','PAYMENT']
        )

    col1, col2 = st.columns(2)
    with col1:
        quarter_mapping = {
            'Q1 (Jan - Mar)': 1,
            'Q2 (Apr - Jun)': 2,
            'Q3 (Jul - Sep)': 3,
            'Q4 (Oct - Dec)': 4
        }
        selected_quarter = quarter_mapping[quarter]
        #table_name = f"aggregate_{data_type.lower()}"
        #column_name = f"{data_type.Capitalize()}_amount"
        
        if data_type == 'INSURANCE':
            query1 = f"SELECT SUM(Insurance_amount) AS 'Insurance Amount', State FROM aggregate_insurance WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY State"
            
            df = excute_query(query1)
            url = ("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw"
            "/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson")
            #response = requests.get(url)
            #data_geojson = response.json()
            data_geojson = fetch_geojson(url)
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                st.warning(f"No data found for {data_type} in {Year} Q{selected_quarter}")
            else:
                map = px.choropleth(
                    df,
                    geojson=data_geojson,
                    locations='State',
                    featureidkey='properties.ST_NM',
                    color='Insurance Amount',
                    color_continuous_scale='RdBu',
                    range_color=[df['Insurance Amount'].min(), df['Insurance Amount'].max()],
                    hover_name='State',
                    hover_data=['Insurance Amount'],
                    title=f"Aggregate {data_type} Amount in {Year} Q{selected_quarter}"
                    
                )
                
                map.update_geos(
                    visible=False,  # Hide the base map (surrounding locations)
                    resolution=50,
                    showcountries=False,
                    showcoastlines=False,
                    projection_type="mercator",
                    lataxis_range=[6, 38],  # Set latitude range to focus on India
                    lonaxis_range=[68, 98]  # Set longitude range to focus on India
                )


                map.update_layout(
                    title="Insurance Amounts by State in India",
                    
                    geo=dict(
                        showframe=False,
                        showcoastlines=False,
                        projection_type='mercator'
                    ),

                    height=800,
                    width=800,
                    #dragmode=False,
                )
                st.plotly_chart(map)
        else:
            
            query2 = f"SELECT SUM(Transaction_amount) AS 'Transaction Amount', State FROM aggregate_transaction WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY State"
            df = excute_query(query2)
            
            url = ("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw"
            "/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson")
            data_geojson = fetch_geojson(url)
            
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                st.warning(f"No data found for {data_type} in {Year} Q{selected_quarter}")
            else:
                map = px.choropleth(
                    df,
                    geojson=data_geojson,
                    locations='State',
                    featureidkey='properties.ST_NM',
                    color='Transaction Amount',
                    color_continuous_scale='RdBu',
                    range_color=[df['Transaction Amount'].min(), df['Transaction Amount'].max()],
                    hover_name='State',
                    hover_data=['Transaction Amount'],
                    title=f"Aggregate {data_type} Amount in {Year} Q{selected_quarter}"
                )
                map.update_geos(
                    visible=False,  # Hide the base map (surrounding locations)
                    resolution=50,
                    showcountries=False,
                    showcoastlines=False,
                    projection_type="mercator",
                    lataxis_range=[6, 38],  # Set latitude range to focus on India
                    lonaxis_range=[68, 98]  # Set longitude range to focus on India
                )
                map.update_layout(
                    title="Transaction Amounts by State in India",
                    geo=dict(
                        showframe=False,
                        showcoastlines=False,
                        projection_type='mercator'
                    ),
                    height=800,
                    width=800
                )
                st.plotly_chart(map)
    with col2:
        if data_type == 'INSURANCE':
            
            st.title("Insurance")
            query3 = f"SELECT SUM(Insurance_count) AS 'Total Policies' FROM aggregate_insurance WHERE Quarter = {selected_quarter} AND Year = {Year}"
            df = (excute_query(query3))
            
            total_insurance_policies = df['Total Policies'].values[0]
            
            if total_insurance_policies is None:
                total_insurance_policies = 0
            else:
                total_insurance_policies = int(total_insurance_policies)
            st.metric("All India Insurance Policies Purchased (Nos.)", total_insurance_policies)
            
            col1, col2 = st.columns(2)
            with col1:
                query4 = f"SELECT SUM(Insurance_amount) AS 'Total Amount' FROM aggregate_insurance WHERE Quarter = {selected_quarter} AND Year = {Year}"
                df = excute_query(query4)
                
                total_insurance_amount = df['Total Amount'].values[0]
                
                if total_insurance_amount is None:
                    total_insurance_amount = 0
                else:
                    total_insurance_amount = int(total_insurance_amount)
                st.metric("Total premium value", f"₹{total_insurance_amount}")
            with col2:
                #query5 = f"SELECT AVG(Insurance_amount) AS 'Total Amount' FROM aggregate_insurance WHERE Quarter = {selected_quarter} AND Year = {Year}"
                #df = excute_query(query5)
                if total_insurance_policies == 0:
                    average_insurance_amount = 0
                else:
                    average_insurance_amount = total_insurance_amount/total_insurance_policies
                    average_insurance_amount = average_insurance_amount
                st.metric("Average premium value", f"₹{average_insurance_amount:.0f}")
                
            tab1,tab2,tab3 = st.tabs(["States","Districts","Postal Codes"]) 
            
            with tab1:
                st.subheader("Top 10 States")
                query6 = f"SELECT State, SUM(Insurance_amount) AS Total_Amount FROM aggregate_insurance WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY State ORDER BY Total_Amount DESC LIMIT 10"
                df = excute_query(query6)
                for index, row in df.iterrows():
                    st.write("", f"{index+1}.{row['State']} - ₹{row['Total_Amount']}")
            with tab2:
                st.subheader("Top 10 Districts")
                query7 = f"SELECT Insurance_district, SUM(Insurance_Amount) AS Total_Amount FROM map_insurance WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY Insurance_district ORDER BY Total_Amount DESC LIMIT 10"
                df = excute_query(query7)
                for index, row in df.iterrows():
                    st.write("", f"{index+1}.{row['Insurance_district']} - ₹{row['Total_Amount']}")
            with tab3:
                st.subheader("Top 10 Postal Codes")
                query8 = f"SELECT Insurance_pincode, SUM(Insurance_amount) AS Total_Amount FROM top_insurance WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY Insurance_pincode ORDER BY Total_Amount DESC LIMIT 10"
                df = excute_query(query8)
                for index, row in df.iterrows():
                    st.write("", f"{index+1}.{row['Insurance_pincode']} - ₹{row['Total_Amount']}")
        else:
            
            st.title("Transaction")
            query5 = f"SELECT SUM(Transaction_amount) AS 'Total Amount' FROM aggregate_transaction WHERE Quarter = {selected_quarter} AND Year = {Year}"
            df = excute_query(query5)
            
            total_transaction_amount = df['Total Amount'].values[0]
            
            if total_transaction_amount is None:
                total_transaction_amount = 0
            else:
                total_transaction_amount = int(total_transaction_amount)
            st.metric("Total transaction value", f"₹{total_transaction_amount}")
            
            col1, col2 = st.columns(2)
            with col1:
                query9 = f"SELECT SUM(Transaction_count) AS 'Total Transactions' FROM aggregate_transaction WHERE Quarter = {selected_quarter} AND Year = {Year}"
                df = excute_query(query9)
                
                total_transaction_count = df['Total Transactions'].values[0]
                
                if total_transaction_count is None:
                    total_transaction_count = 0
                else:
                    total_transaction_count = int(total_transaction_count)
                st.metric("Total transactions", total_transaction_count)
            with col2:
                if total_transaction_count == 0:
                    average_transaction_amount = 0
                else:
                    average_transaction_amount = total_transaction_amount/total_transaction_count
                    average_transaction_amount = average_transaction_amount
                st.metric("Average transaction value", f"₹{average_transaction_amount:.0f}")
            st.title("Categories")
            query10 = f"SELECT Transaction_type, SUM(Transaction_amount) AS category_amount FROM aggregate_transaction WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY Transaction_type ORDER BY category_amount DESC"
            df = excute_query(query10)
            for index, row in df.iterrows():
                st.write("", f"{index+1}.{row['Transaction_type']} - ₹{row['category_amount']}")
            
            tab1,tab2,tab3 = st.tabs(["States","Districts","Postal Codes"])
            
            with tab1:
                st.subheader("Top 10 States")
                query11 = f"SELECT State, SUM(Transaction_amount) AS Total_Amount FROM aggregate_transaction WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY State ORDER BY Total_Amount DESC LIMIT 10"
                df = excute_query(query11)
                for index, row in df.iterrows():
                    st.write("", f"{index+1}.{row['State']} - ₹{row['Total_Amount']}")
            with tab2:
                st.subheader("Top 10 Districts")
                query12 = f"SELECT Transaction_district, SUM(Transaction_Amount) AS Total_Amount FROM map_transaction WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY Transaction_district ORDER BY Total_Amount DESC LIMIT 10"
                df = excute_query(query12)
                for index, row in df.iterrows():
                    st.write("", f"{index+1}.{row['Transaction_district']} - ₹{row['Total_Amount']}")
            with tab3:
                st.subheader("Top 10 Postal Codes")
                query13 = f"SELECT Transaction_pincode, SUM(Transaction_amount) AS Total_Amount FROM top_transaction WHERE Quarter = {selected_quarter} AND Year = {Year} GROUP BY Transaction_pincode ORDER BY Total_Amount DESC LIMIT 10"
                df = excute_query(query13)
                for index, row in df.iterrows():
                    st.write("", f"{index+1}.{row['Transaction_pincode']} - ₹{row['Total_Amount']}")
                    
if selected == "INSIGHTS":
    q1 = f"""
    SELECT User_Device, SUM(User_count) AS User_count FROM aggregate_user
    GROUP BY User_Device ORDER BY User_count DESC LIMIT 10;
    
    """
    df = excute_query(q1)
    st.title("Top 10 User Devices")
    
    #bar chart
    fig = px.bar(df, x="User_Device", y="User_count", color="User_Device", title="Top 10 User Devices")
    st.plotly_chart(fig, use_container_width=True)
    
    #pie chart
    fig = px.pie(df, values="User_count", names="User_Device", title="Top 10 User Devices")
    st.plotly_chart(fig)
    
    # User Registration in districts
    q2 = f"""
    SELECT User_district, SUM(User_registration) AS User_registration FROM map_user
    GROUP BY User_district ORDER BY User_registration DESC LIMIT 10;
    
    """
    df = excute_query(q2)
    st.title("Top 10 User Registration in Districts")
    
    #bar chart
    fig = px.bar(df, x="User_district", y="User_registration", color="User_registration", title="Top 10 User Registration in Districts")
    st.plotly_chart(fig, use_container_width=True)
    
    #pie chart
    fig = px.pie(df, values="User_registration", names="User_district", title="Top 10 User Registration in Districts")
    st.plotly_chart(fig)
    
    district = f"SELECT DISTINCT User_district FROM map_user"
    df = excute_query(district)
    district_list = df["User_district"].tolist()
    selected_district = st.selectbox("Select a District", district_list)
    q3 = f"""
    SELECT User_district, SUM(User_registration) AS User_registration FROM map_user 
    WHERE User_district = '{selected_district}'
    GROUP BY User_district;
    
    """
    st.write(f"Total User Registration in {selected_district}", excute_query(q3)["User_registration"].values[0])
    
    # User App Open in districts
    q4 = f"""
    SELECT User_district, SUM(User_appOpen) AS User_appOpen FROM map_user
    GROUP BY User_district ORDER BY User_appOpen DESC LIMIT 10;
    
    """
    df = excute_query(q4)
    st.title("Top 10 User App Open in Districts")
    
    #bar chart
    fig = px.bar(df, x="User_district", y="User_appOpen", color="User_appOpen", title="Top 10 User App Open in Districts")
    st.plotly_chart(fig, use_container_width=True)
    
    #pie chart
    fig = px.pie(df, values="User_appOpen", names="User_district", title="Top 10 User App Open in Districts")
    st.plotly_chart(fig)
    
