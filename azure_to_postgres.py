# Requires python 3.6 or above

import os
import pandas as pd
from sqlalchemy import create_engine
import yaml
import csv
from csv import reader

# Credentials for xgdwh_core_p_lab
driver = ""
username = ""
password = ""
host = ""
port = ""
database = ""
schema_name = ""
LOCAL_BLOB_PATH = ""
with open('creds.yml') as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
    driver = data['driver']
    username = data['username']
    password = data['password']
    host = data['host']
    port = data['port']
    database = data['database']
    schema_name = data['schema_name']
    LOCAL_BLOB_PATH = data['LOCAL_BLOB_PATH']
# CSV Config File
csvFile = LOCAL_BLOB_PATH + '/tables_to_be_loaded.csv'
rows = []
    
class AzureBlobFileDownloader(object):

  def __init__(self):
    print("Intializing AzureBlobFileDownloader")

  def __getitem__(self, items):
      print (type(items), items)   

  def save_blob(self,file_name,file_content):
    # Get full path to the file
    download_file_path = os.path.join(LOCAL_BLOB_PATH, file_name)
    print(download_file_path)
    
  def csv_to_postgres(self):
    directory = LOCAL_BLOB_PATH
    engine = create_engine('{0}://{1}:{2}@{3}:{4}/{5}'.format(driver,username,password,host,port,database))
    print('Die Verbindung mit der Datenbank wurde gesetzt')
    for file in os.listdir(directory):
     size = len(file)
     filename = file[:size-4]
     print(filename)
     full_path =  directory + "/" + file
     if file.endswith('.csv') and file != 'tables_to_be_loaded.csv': 
      with open(csvFile, "r", encoding="utf-8") as outputfile:
       table_name = csv.reader(outputfile)
       for row in table_name: 
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute("select count(*) from information_schema.tables where table_schema = " + "'" + schema_name + "'" + " and table_name = " + "'" + row[0] + "'")
        results = cursor.fetchone()[0]  
        if results == 1:
         if filename == row[0]:
          engine.execute("TRUNCATE TABLE " + schema_name + "." + row[0])
          print("TRUNCATE TABLE " + schema_name + "." + row[0])
          for df in pd.read_csv(full_path, encoding='utf-8', iterator = True, chunksize=1000):
           df.to_sql(row[0], engine, if_exists='append', index=False, schema=schema_name)
          print('Die Tabelle ' + row[0] + ' wurde befüllt')
          os.remove(os.path.join(directory, file))
          print('Die Datei ' + file + ' wurde gelöscht')

# Initialize class and upload files
azure_blob_file_downloader = AzureBlobFileDownloader()
azure_blob_file_downloader.csv_to_postgres()
