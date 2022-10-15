
#%%
import requests
import glob
import pandas as pd
from datetime import datetime

#%% Get Data
url_json = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/'

file_json1 = 'bank_market_cap_1.json'
r_json1 = requests.get(url_json+file_json1)
open(file_json1, 'wb').write(r_json1.content)

file_json2 = 'bank_market_cap_2.json'
r_json2 = requests.get(url_json+file_json2)
open(file_json2, 'wb').write(r_json2.content)

url_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/'

file_csv = 'exchange_rates.csv'
r_csv = requests.get(url_csv+file_csv)
open(file_csv, 'wb').write(r_csv.content)

#%% Extract Function: json to data frame
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

#%% Extract Funcion: multiple files to single data frame
def extract():
    df_json = pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
    for jsonfile in glob.glob("*.json"):
        df_json = pd.concat([df_json, extract_from_json(jsonfile)], ignore_index=True)
        print(f"Extracted data from {jsonfile}" )
    return df_json

#%% Extracting CSV file into data frame and renaming column
df_csv = pd.read_csv(open('exchange_rates.csv'))
df_csv.rename(columns = {'Unnamed: 0': 'Currency'}, inplace = True)

#%% Getting the GBP rate
exchange_rate = df_csv[df_csv['Currency'] == 'GBP'].iloc[0,1]
exchange_rate

#%% Transform Function: Convert the data frame currently in USD into GBP values
def transform(df):
    df['Market Cap (US$ Billion)'] = round(df['Market Cap (US$ Billion)'] * exchange_rate, 3)
    df = df.rename(columns ={'Market Cap (US$ Billion)': 'Market Cap (GBP$ Billion)'})
    return df

#%% Load Function: data frame into CSV file
def load(file, data):
    data.to_csv(file, sep=';', index=False)


#%% Log Function to keep track
def log(msg):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    #with open("logfile.txt","a") as f:
        #f.write(timestamp + ',' + msg + '\n')
    print(timestamp + ',' + msg)
    
#%% 
log("ETL Job Started")

#%%
log("Extract phase Started")
data = extract()
data.head()

#%%
log("Extract phase Ended")

#%%
log("Transform phase Started")
data_transformed = transform(data)
data_transformed.head()

#%%
log("Transform phase Ended")

#%%
log("Load phase Started")
load('market_cap.csv', data_transformed)


#%%
log("Load phase Ended")
