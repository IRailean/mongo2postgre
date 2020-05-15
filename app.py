from flask import Flask, Blueprint, request, jsonify
from flask_apscheduler import APScheduler
import pymongo
import logging
import psycopg2
import psycopg2.extras
import time
import pandas as pd
import io
import os
import query
import config
from sqlalchemy import create_engine

# Debug
import datetime

app = Flask(__name__)
scheduler = APScheduler()


@app.route("/")
def index():
    return "route"


class Data():
    def __init__(self, date, table_names):
        self.current_time = date
        self.table_names = table_names
        self.last_update_time = None
        self.postgre_connection = None
        self.postgre_engine = None
        self.postgre_engine_connection = None
        self.mongo_connection = None

        self.tables = {}
        self.tables['orders'] = {'create_query' : query.orders_create, 'upsert_query' : query.orders_upsert, 'template' : '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'}
        self.tables['users'] = {'create_query' : query.users_create, 'upsert_query' : query.users_upsert, 'template': '(%s,%s,%s,%s,%s,%s,%s)'}

    def connectDBs(self):
        try:
            mongo_connect = "mongodb://" + mongo_user + ":" + mongo_pass + "@" + mongo_host + ":" + str(mongo_port) + "/" + mongo_db
            client = pymongo.MongoClient(mongo_connect)

            logging.debug("Init collections...")
            self.mongo_connection  = client[mongo_db]

        except (Exception) as error:
            print("Error occured " + error)

        try:
            self.postgre_connection = psycopg2.connect(user=postgres_user,
                                            password=postgres_pass,
                                            host=postgres_host,
                                            port=postgres_port,
                                            database=postgres_db)
        except (Exception) as error:
            print("Error occured " + error)

        try:
            postgres_connect = "postgresql+psycopg2://" + postgres_user + ":" + postgres_pass + "@" + postgres_host + ":" + str(postgres_port) + "/" + postgres_db
            engine = create_engine(postgres_connect)
            self.postgre_engine = engine
            self.postgre_engine_connection = engine.raw_connection()
        except (Exception) as error:
            print("Error occured " + error)

    def populateDB(self):
        self.connectDBs()
        [self.populateTable(table_name) for table_name in self.table_names]

    def populateTable(self, table_name):
        mongoDBData = self.mongo_connection[table_name].find({"created_at": {"$lt": self.current_time }})

        if mongoDBData.count() == 0:
            # The table will be created during the next update
            return
        columns = list(mongoDBData[0].keys())

        df = pd.DataFrame(mongoDBData, columns=columns[1:])
        
        df.head(0).to_sql(table_name, self.postgre_engine, if_exists='replace', index=False) #truncates the table

        with self.postgre_engine_connection.cursor() as cursor:
            output = io.StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cursor.copy_from(output, table_name, null="") # null values become ''
            sql_query = "ALTER TABLE " + table_name + " ADD PRIMARY KEY (" + columns[1] + ")" # id
            cursor.execute(sql_query)
            self.postgre_engine_connection.commit()
        self.last_update_time = self.current_time

    def updateDB(self):
        [self.updateTable(table_name) for table_name in self.table_names]

    def updateTable(self, table_name):
        mongoDBData = self.mongo_connection[table_name].find({
            "created_at": {
                "$gte": self.last_update_time,
                "$lt": self.current_time
            }
            })
        values = []
        for o in mongoDBData:
            o.pop("_id", None)
            l = list(o.values())
            print(l)
            for idx, value in enumerate(l):
                if isinstance(value, datetime.datetime):
                    l[idx] = value.strftime('%Y/%m/%d %H:%M:%S')
                if str(value) == "":
                    l[idx] = None
            values.append(tuple(l))

        print("Rows need to be inserted: ", len(values))
        if values != []: 
    
            logging.debug("Got mongoDBdata...")
            with self.postgre_connection.cursor() as cursor:
                logging.debug("Insert into goparrot")
                cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
                if not cursor.fetchone()[0]:
                    print("Table does not exist")
                    create_query = self.tables[table_name]['create_query']
                    cursor.execute(create_query)
                    self.postgre_connection.commit()

                insert_query = self.tables[table_name]['upsert_query']

                psycopg2.extras.execute_values (
                    cursor, insert_query, values, template=self.tables[table_name]['template'], page_size=100
                )
                self.postgre_connection.commit()
                rowcount = cursor.rowcount
                print(rowcount, " rows updated")

    def scheduledTask(self):
            self.last_update_time = self.current_time
            self.current_time += datetime.timedelta(hours=24)
            print("Task running")
            self.updateDB()

if __name__ == '__main__':
    postgres_user = config.POSTGRES_USER
    postgres_pass = config.POSTGRES_PASSWORD
    postgres_host = config.POSTGRES_HOST
    postgres_port = config.POSTGRES_PORT
    postgres_db = config.POSTGRES_DB

    mongo_user = config.MONGO_USER
    mongo_pass = config.MONGO_PASSWORD
    mongo_host = config.MONGO_HOST
    mongo_port = config.MONGO_PORT
    mongo_db = config.MONGO_DB

    logging.basicConfig(level=logging.DEBUG)
    
    start_date = datetime.datetime(2019, 6, 24)
    tables = ['orders', 'users']

    data = Data(start_date, tables)
    data.populateDB()
    scheduler.add_job(id = "Scheduled task", func = data.scheduledTask, trigger = 'interval', seconds = 5)
    scheduler.start()
    app.run(host = 'localhost', port = 8080)