import requests
from bs4 import BeautifulSoup
import sqlite3
from collections import namedtuple
import queue
import time
import pandas as pd

url = 'https://gml.noaa.gov/aggi/aggi.html'
response = requests.get(url)
tb = pd.read_html(url)[1]
print(tb)

'''used bs4 to get headers only'''
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find_all('table')[1]
headers = [header.text.strip('') for header in table.find_all('th')]

# filter out the * symbol
'''Headers grab the data of the first row'''
headers = headers[4:9]


# print(headers)


class Thread:
    def __init__(self):
        self.sqliteConnection = None
        pass

    def create_named_tuple(self, db, type1, type2):
        return namedtuple(db, [type1, type2])

    def query__builder(self, dbname, datatypes):
        add_db_types = ''
        print(datatypes[0])
        '''Getting information'''
        i = 0
        for (x, y) in zip(datatypes[0], datatypes[1]):
            if i == len(datatypes[0]) - 1:
                add_db_types += x + ' ' + y
            else:
                add_db_types += x + ' ' + y + ','
            i += 1
        '''Use a string to input '''
        build = 'CREATE TABLE {}({})'.format(dbname, add_db_types)
        print('Here is the', build)
        return build

    def connect(self, dbname):
        try:
            self.sqliteConnection = sqlite3.connect(dbname)
            cursor = self.sqliteConnection.cursor()
            print("Database created and Successfully Connected to SQLite")
            select_Query = "select sqlite_version();"
            print("Search query: ", select_Query)
            cursor.execute(select_Query)
            record = cursor.fetchall()
            print("SQLite Database Version is: ", record)
        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)

    def insert_into_db(self, table_q, new):
        try:
            print(table_q)
            cursor = self.sqliteConnection.cursor()
            cursor.execute(table_q, tuple(new))
            self.sqliteConnection.commit()
            print("Inserted successfully into table")
        except sqlite3.Error as error:
            print("Failed to insert: ", error)

    def insert_query(self, table_name, fields, values):
        query = "INSERT INTO {} ({}) VALUES ({})".format(
            table_name, ", ".join(fields), ", ".join("?" * len(values)))
        print('Here is the query', query)
        return query, values

    def table(self, query):
        try:
            cursor = self.sqliteConnection.cursor()
            cursor.execute(query)
            print("Table query: ", query)
            self.sqliteConnection.commit()
            print("SQLite table created")
        except sqlite3.Error as error:
            print("Table exists: ", error)

    def createTypes(self, *name):
        data = []
        for i in name:
            data.extend(i)
        return data


def main():
    t = Thread()
    t.connect('C02.db')
    '''Creating types'''
    list_for_dt = ['STRING', 'INTEGER']
    list_for_df = ['name', 'data_type']
    types_field = t.createTypes(headers)

    '''Creating DB'''
    db_named_tup = t.create_named_tuple('Database', 'Data_fields', 'Data_types')
    db_for_table = db_named_tup(list_for_df, list_for_dt)
    q = t.query__builder('DB', db_for_table)
    t.table(q)
    '''inserting and extracting data'''
    for e in headers:
        ins, vals = t.insert_query('DB', list_for_df,list_for_df)  # 3rd parameter is the length
        # print(ins)
        df = tb.loc[:, (['Global Radiative Forcing (W m-2)'], [e])]
        # print(df)
        value = ""
        track = 0
        for a in range(len(df)):
            try:
                if df.values[a].size == 1 and float(df.values[a]) and (track > 10) and (track < 41):
                    temp = float(df.values[a])
                    value = " ".join([value, str(temp)])
                    print(value)
            except ValueError:
                continue
            track += 1
        c = db_named_tup(e, value)
        t.insert_into_db(ins, c)



main()
