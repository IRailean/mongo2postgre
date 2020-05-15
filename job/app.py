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
    def __init__(self, date):
        self.current_time = date
        self.last_update_time = None
        self.synced = False
        self.collection_orders = None
        self.collection_users = None

    def connectMongo(self):
        client = pymongo.MongoClient("mongodb://admin:admin@localhost:27017/database")

        logging.debug("Init collections...")
        database = client["database"]

        self.collection_orders = database["orders"]
        self.collection_users  = database["users"]

    def scheduledTask(self):
        self.last_update_time = self.current_time
        self.current_time += datetime.timedelta(hours=24)
        print("Task running")
        self.updateDB()

    def populateDB(self):
        self.connectMongo()
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="Motocross971",
                                            host="localhost",
                                            port="5432",
                                            database=config.POSTGRES_DB)
            with connection.cursor() as cursor:
                mongoDBData = self.collection_orders.find({
                    "created_at": {
                        "$lt": self.current_time
                    }
                    })
                columns = ['_id', 'id', 'created_at', 'date_tz', 'item_count', 'order_id', 'receive_method', 'status', 'store_id', 'subtotal', \
                    'tax_percentage', 'total', 'total_discount', 'total_gratuity', 'total_tax', \
                     'updated_at', 'user_id', 'fulfillment_date_tz']

                df = pd.DataFrame(mongoDBData, columns=columns[1:])
                engine = create_engine('postgresql+psycopg2://postgres:Motocross971@localhost:5432/postgres')
                print(df.head())
                df.head(0).to_sql('goparrot', engine, if_exists='replace',index=False) #truncates the table
                df.fillna(" ", inplace=True)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cursor.copy_from(output, 'goparrot', null="") # null values become ''
                cursor.execute("ALTER TABLE goparrot ADD PRIMARY KEY (id)")
                connection.commit()
                self.last_update_time = self.current_time
        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to insert record into mobile table", error)

    def updateDB(self):
        
        mongoDBData = self.collection_orders.find({
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

        table = 'goparrot'
        print("Rows need to be inserted: ", len(values))
        if values != []: 
    
            logging.debug("Got mongoDBdata...")
            connection = psycopg2.connect(user="postgres",
                                            password="Motocross971",
                                            host="localhost",
                                            port="5432",
                                            database=config.POSTGRES_DB)
            with connection.cursor() as cursor:
                logging.debug("Insert into goparrot")

                insert_query = 'INSERT INTO goparrot (id, created_at, date_tz, item_count, order_id, receive_method, status, store_id, subtotal, \
                 tax_percentage, total, total_discount, total_gratuity, total_tax, \
                     updated_at, user_id, fulfillment_date_tz) VALUES %s ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at'

                psycopg2.extras.execute_values (
                    cursor, insert_query, values, template='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', page_size=100
                )
                connection.commit()
                rowcount = cursor.rowcount
                print(rowcount, " rows updated")



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    cur_date = datetime.datetime(2019, 7, 19)
    data = Data(cur_date)
    data.populateDB()
    scheduler.add_job(id = "Scheduled task", func = data.scheduledTask, trigger = 'interval', seconds = 5)
    scheduler.start()
    app.run(host = 'localhost', port = 8080)