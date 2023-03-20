import sqlite3
import pandas as pd
import threading
import collections
import queue
import requests
import time
from bs4 import BeautifulSoup
from Graph import *
url = 'https://gml.noaa.gov/aggi/aggi.html'


class DB:
    def __init__(self):
        self.sqliteConnection = None
        self._dict = {}

    def connect(self, dbname):
        try:
            self.sqliteConnection = sqlite3.connect(dbname, check_same_thread=False)
            cursor = self.sqliteConnection.cursor()
            print("\nDatabase created and Successfully Connected to SQLite")
            select_Query = "select sqlite_version();"
            print("Search query: ", select_Query)
            cursor.execute(select_Query)
            record = cursor.fetchall()
            print("SQLite Database Version is: ", record)
        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
        return dbname

    def table(self, query):
        try:
            cursor = self.sqliteConnection.cursor()
            cursor.execute(query)
            print("Table query: ", query)
            self.sqliteConnection.commit()
            print("SQLite table created")
            return True
        except sqlite3.Error as error:
            print("Table exists: ", error)
            return False

    def insert(self, table_q, tup):
        try:
            cursor = self.sqliteConnection.cursor()
            cursor.execute(table_q, tup)
            table_q = query_builder('''INSERT INTO ''', '''Database ''', tup._fields, tup)
            print("Search query: ", table_q)
            self.sqliteConnection.commit()
            print("Inserted successfully into table\n")
        except sqlite3.Error as error:
            print("Failed to insert: ", error)

    def search(self, value, query):
        cursor = self.sqliteConnection.cursor()
        sel = query.format(value)
        print("Search query: ", sel)
        cursor.execute(sel)
        result = cursor.fetchall()
        return result


def query_builder(cmd, name, field, values):
    command = cmd + name + str(field) + ' ' + str(values)
    return command


def insert_query_builder(table_name, fields):
    query = "INSERT INTO {} ({}) VALUES ({})".format(
        table_name, ", ".join(fields), ", ".join("?" * len(fields)))
    print('Here is the query', query)
    return query


def thread_function(name, data, q):
    screenlock = threading.Semaphore(value=1)
    with screenlock:
        print("Agent %s: starting" % name)
        search_query = 'SELECT data FROM DB WHERE name == "{0}"'
        time.sleep(0.7)
        info = data.search(name, search_query)
        print(info)
        q.put(info)
        print("Agent %s: finishing" % name)
    return


def thread_agent(name, q, threads, data, url):
    for i in name:
        temp = i
        t = threading.Thread(target=thread_function, args=(temp, data, q))
        time.sleep(0.1)
        t.start()
        threads.append(t)
    return threads


# pass in tuple
def query__builder_table(dbname, datatypes):
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


response = requests.get(url)
tb = pd.read_html(url)[1]
print(tb)

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


def main():
    url = "https://gml.noaa.gov/aggi/aggi.html"
    data = DB()
    data.connect('Database.db')
    tables = pd.read_html(url)

    '''Creating tuple for table'''
    list_for_dt = ['STRING', 'INTEGER']
    list_for_df = ['name', 'data']
    builder_table = collections.namedtuple('VALUES', ['name', 'data'])
    values = builder_table(name=list_for_df, data=list_for_dt)
    table_q = query__builder_table('DB', values)

    columns_db = collections.namedtuple('VALUES', ['name', 'data'])
    print(table_q)

    tb = tables[1]
    if data.table(table_q):
        for i in headers:
            # table_query_insert = '''INSERT INTO DB ('name', 'data') VALUES(?, ?)'''
            table_query_insert = insert_query_builder('DB', values.name)
            print(table_query_insert)
            df = tb.loc[:, (['Global Radiative Forcing (W m-2)'], [i])]
            # print(float(df.values[0]))
            value = ""
            track = 0
            for a in range(len(df)):
                try:
                    if float(df.values[a]) and (track > 10) and (track < 41):
                        temp = float(df.values[a])
                        value = " ".join([value, str(temp)])
                except ValueError:
                    continue
                track += 1
            col_data = columns_db(i, value)
            data.insert(table_query_insert, col_data)

    q = queue.Queue()
    threads = []

    threads = thread_agent(headers, q, threads, data, url)

    for j in threads:
        j.join()

    thread_result = []

    while not q.empty():
        thread_result.append(q.get())

    g = Graph()
    g.graph_plot(thread_result,headers, 'Year', 'Data')

# print(headers)




main()
