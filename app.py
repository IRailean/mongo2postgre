from flask import Flask
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
from argparse import ArgumentParser

import datetime

app = Flask(__name__)
scheduler = APScheduler()

def make_parser():
    parser = ArgumentParser(description="Train Single Shot MultiBox Detector"
                                        " on COCO")
    parser.add_argument('--postgres-user', '-pguser', type=str, default=config.POSTGRES_USER, required=False,
                        help='specify postgres user')
    parser.add_argument('--postgres-password', '-pgpass', type=str, default=config.POSTGRES_PASSWORD, required=False,
                        help='specify postgres password')
    parser.add_argument('--postgres-host', '-pghost', type=str, default=config.POSTGRES_HOST, required=False,
                        help='specify postgres host')
    parser.add_argument('--postgres-port', '-pgport', type=str, default=config.POSTGRES_PORT, required=False,
                        help='specify postgres port')
    parser.add_argument('--postgres-database', '-pgdb', type=str, default=config.POSTGRES_DB, required=False,
                        help='specify postgres database')
    
    parser.add_argument('--mongo-user', '-muser', type=str, default=config.MONGO_USER, required=False,
                        help='specify mongo user')
    parser.add_argument('--mongo-password', '-mpass', type=str, default=config.MONGO_PASSWORD, required=False,
                        help='specify mongo password')
    parser.add_argument('--mongo-host', '-mhost', type=str, default=config.MONGO_HOST, required=False,
                        help='specify mongo host')
    parser.add_argument('--mongo-port', '-mport', type=str, default=config.MONGO_PORT, required=False,
                        help='specify mongo port')
    parser.add_argument('--mongo-database', '-mdb', type=str, default=config.MONGO_DB, required=False,
                        help='specify mongo database')
    return parser


@app.route("/")
def index():
    return "Hello, GoParrot!"


class Runner():
    def __init__(self, date, table_names):
        self.current_time = date
        self.table_names = table_names
        self.last_update_time = None
        self.postgre_connection = None
        self.postgre_engine = None
        self.mongo_connection = None

        self.tables = {}
        self.tables['orders'] = {'create_query' : query.orders_create, 'upsert_query' : query.orders_upsert, 'template' : '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'}
        self.tables['users'] = {'create_query' : query.users_create, 'upsert_query' : query.users_upsert, 'template': '(%s,%s,%s,%s,%s,%s,%s)'}

    def connectDBs(self):
        try:
            mongo_connect = "mongodb://" + mongo_user + ":" + mongo_pass + "@" + mongo_host + ":" + str(mongo_port) + "/" + mongo_db
            logging.info("Connecting to mongo database: %s", mongo_connect)
            client = pymongo.MongoClient(mongo_connect)

            logging.debug("Init collections...")
            self.mongo_connection  = client[mongo_db]

            logging.debug("Connected OK to mongo database: %s", mongo_connect)
        except (Exception) as error:
            logging.debug("Failed to connect to mongo database: %s", mongo_connect)

        try:
            postgres_connect = "postgresql://" + postgres_user + ":" + postgres_pass + "@" + postgres_host + ":" + str(postgres_port) + "/" + postgres_db
            logging.info("Connecting to postgre database: %s", postgres_connect)
            self.postgre_connection = psycopg2.connect(user=postgres_user,
                                            password=postgres_pass,
                                            host=postgres_host,
                                            port=postgres_port,
                                            database=postgres_db)

            logging.debug("Connected OK to postgre database: %s", postgres_connect)
        except (Exception) as error:
            logging.debug("Failed to connect to postgre database: %s", postgres_connect)

        try:
            postgres_connect_engine = "postgresql+psycopg2://" + postgres_user + ":" + postgres_pass + "@" + postgres_host + ":" + str(postgres_port) + "/" + postgres_db
            logging.info("Creating postgre engine: %s", postgres_connect_engine)
            engine = create_engine(postgres_connect_engine)
            self.postgre_engine = engine

            logging.info("Created OK postgre engine: %s", postgres_connect_engine)
        except (Exception) as error:
            logging.debug("Failed to create postgre engine: %s", postgres_connect_engine)

    def populateDB(self):
        logging.debug("Populating database")
        self.connectDBs()
        [self.populateTable(table_name) for table_name in self.table_names]

    def populateTable(self, table_name):
        logging.info("Populating table %s", table_name)
        try:
            mongoDBData = self.mongo_connection[table_name].find({"created_at": {"$lt": self.current_time }})
        except (Exception) as error:
            logging.error("Could not get data from mongoDB for table %s", table_name)
            return

        if mongoDBData.count() == 0:
            # The table will be created during the next update
            logging.warning("%s table is empty. Could not populate", table_name)
            return
        columns = list(mongoDBData[0].keys())

        df = pd.DataFrame(mongoDBData, columns=columns[1:])
        try:
            df.head(0).to_sql(table_name, self.postgre_engine, if_exists='replace', index=False) #truncates the table
        except (Exception) as error:
            logging.error("Could not add header to the table %s", table_name)

        try:
            with self.postgre_connection.cursor() as cursor:
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cursor.copy_from(output, table_name, null="") # null values become ''
                sql_query = "ALTER TABLE " + table_name + " ADD PRIMARY KEY (" + columns[1] + ")" # id
                cursor.execute(sql_query)
                self.postgre_connection.commit()
        except (Exception) as error:
            logging.error("Could not populate table %s due to connection issues", table_name)
        self.last_update_time = self.current_time

    def updateDB(self):
        logging.info("Updating database, current time: %s, last update time: %s", self.current_time, self.last_update_time)
        [self.updateTable(table_name) for table_name in self.table_names]

    def updateTable(self, table_name):
        logging.info("Updating table %s", table_name)
        try:
            mongoDBData = self.mongo_connection[table_name].find({"$or" : [{"created_at": {"$gte": self.last_update_time,"$lt": self.current_time}},
                                                                       {"updated_at": {"$gte": self.last_update_time,"$lt": self.current_time}}]})
        except (Exception) as error:
            logging.error("Could not get data from mongoDB for table %s", table_name)
            return

        values = []
        for o in mongoDBData:
            # Remove _id added in mongoDB
            o.pop("_id", None)
            values_as_list = list(o.values())
            for idx, value in enumerate(values_as_list):
                if isinstance(value, datetime.datetime):
                    values_as_list[idx] = value.strftime('%Y/%m/%d %H:%M:%S')
                if str(value) == "":
                    values_as_list[idx] = None
            values.append(tuple(values_as_list))

        logging.info("Rows need to be inserted: %s for table %s", len(values), table_name)
        if values != []:  
            try:
                with self.postgre_connection.cursor() as cursor:
                    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
                    if not cursor.fetchone()[0]:
                        logging.warning("Table %s does not exist, create it", table_name)
                        create_query = self.tables[table_name]['create_query']
                        cursor.execute(create_query)
                        self.postgre_connection.commit()

                    upsert_query = self.tables[table_name]['upsert_query']

                    logging.debug("Upsert values in table %s", table_name)

                    psycopg2.extras.execute_values (
                        cursor, upsert_query, values, template=self.tables[table_name]['template'], page_size=100
                    )
                    self.postgre_connection.commit()
                    rowcount = cursor.rowcount
                    logging.debug("%s rows has been updated", str(rowcount), )
            except (Exception) as error:
                logging.error("Could not update table %s due to connection issues", table_name)

    def scheduledTask(self):
        logging.debug("Scheduled task runned")
        self.last_update_time = self.current_time
        self.current_time += datetime.timedelta(hours=24)
        self.updateDB()

if __name__ == '__main__':

    parser = make_parser()
    args = parser.parse_args()

    postgres_user = args.postgres_user
    postgres_pass = args.postgres_password
    postgres_host = args.postgres_host
    postgres_port = args.postgres_port
    postgres_db   = args.postgres_database

    mongo_user = args.mongo_user
    mongo_pass = args.mongo_password
    mongo_host = args.mongo_host
    mongo_port = args.mongo_port
    mongo_db   = args.mongo_database

    logging.basicConfig(level = logging.DEBUG)

    start_date = datetime.datetime(2019, 6, 24)
    tables = ['orders', 'users']

    runner = Runner(start_date, tables)
    runner.populateDB()
    scheduler.add_job(id = "Scheduled task", func = runner.scheduledTask, trigger = 'interval', seconds = 5)
    scheduler.start()
    app.run(debug = False, host = config.HOST, port = 8080)