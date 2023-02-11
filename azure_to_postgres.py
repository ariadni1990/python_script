# download_blobs.py
# Python program to bulk download blob files from azure storage
# Uses latest python SDK() for Azure blob storage
# Requires python 3.6 or above

from contextlib import nullcontext
import os
import logging
import string
from tkinter import NUMERIC
from tokenize import Number
from unicodedata import numeric
from xml.dom.pulldom import CHARACTERS
import pandas as pd
from sqlalchemy import create_engine, null
import psycopg2
import yaml
import csv
from csv import reader
import time
import pytz
from tqdm import tqdm
from tqdm.notebook import tqdm_notebook
import numpy as np
import sys

LOGFORMAT = "%(asctime)s %(message)s"  # Your format
# Your default level, usually set to warning or error for production
DEFAULT_LEVEL = "info"
LEVELS = {
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR}

logging.basicConfig(format=LOGFORMAT, level=LEVELS[DEFAULT_LEVEL])
logging.info('the script has started')
# Local folder where the files should be downloaded
LOCAL_BLOB_PATH = './data'
print(os.getcwd())
# CSV Config File
csvFile = './data/tables_to_be_loaded.csv'
rows = []
# Credentials for xgdwh_core_p_lab
driver = ""
username = ""
password = ""
host = ""
port = ""
database = ""
schema_name = ""
with open('creds.yml') as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
    driver = data['driver']
    username = data['username']
    password = data['password']
    host = data['host']
    port = data['port']
    database = data['database']
    schema_name = data['schema_name']


class AzureBlobFileDownloader(object):
    def __init__(self):
        print("Intializing AzureBlobFileDownloader")

    def __getitem__(self, items):
        print(type(items), items)

    def save_blob(self, file_name, file_content):
        # Get full path to the file
        download_file_path = os.path.join(LOCAL_BLOB_PATH, file_name)
        print(download_file_path)

    def csv_to_postgres(self):
        directory = LOCAL_BLOB_PATH
        engine = create_engine('{0}://{1}:{2}@{3}:{4}/{5}?sslmode=require&keepalives=1'.format(
            driver, username, password, host, port, database), pool_pre_ping=True)
        print('Die Verbindung mit der Datenbank wurde gesetzt')
        for file in os.listdir(directory):
            size = len(file)
            filename = file[:size-4]
            print(filename)
            full_path = directory + "/" + file
            lines_in_file = open(full_path, 'r', encoding="utf8").readlines()
            number_of_lines = len(lines_in_file)
            if file.endswith('.csv') and file != 'tables_to_be_loaded.csv':
                with open(csvFile, "r", encoding="utf-8") as outputfile:
                    table_name = csv.reader(outputfile)
                    for row in table_name:
                        connection = engine.raw_connection()
                        cursor = connection.cursor()
                        cursor.execute("select count(*) from information_schema.tables where table_schema = " +
                                       "'" + schema_name + "'" + " and table_name = " + "'" + row[0] + "'")
                        results = cursor.fetchone()[0]
                        print(results)
                        if results == 1:
                            if filename == row[0]:
                                engine.execute(
                                    "TRUNCATE TABLE " + schema_name + "." + row[0])
                                print('TRUNCATE TABLE lab.' + row[0])
                                pbar = tqdm(range(1, number_of_lines))
                                counter = 0
                                with engine.connect() as conn:
                                    try:
                                        for df in pd.read_csv(full_path, encoding='utf-8', iterator=True, escapechar='\\', chunksize=1000):
                                            counter += 1
                                            print(
                                                "Chunk # " + str(counter) + " in Bearbeitung...")
                                            df.to_sql(
                                                row[0], conn, if_exists='append', index=False, schema=schema_name)
                                            engine.dispose()
                                            # for i in pbar:
                                            # pbar.set_description("%s rows processed" % i)
                                    except Exception as e:
                                        logging.error(str(e))
                                        raise
                                    except Exception as warn:
                                        logging.warning(str(warn))
                                        raise
                    print('Die Tabelle ' + row[0] + ' wurde befüllt')

                    # time.sleep(0.0000001)
                    # os.remove(os.path.join(directory, file))
                    # print('Die Datei ' + file + ' wurde gelöscht')


# Initialize class and upload files
azure_blob_file_downloader = AzureBlobFileDownloader()
azure_blob_file_downloader.csv_to_postgres()
logging.info('the script has finished')
