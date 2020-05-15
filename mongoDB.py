import pymongo
import pandas as pd
import logging
import datetime
import numpy as np
import config

def populateDB():
    logging.debug("Connecting to the database...")
    mongo_connect = "mongodb://" + mongo_user + ":" + mongo_pass + "@" + mongo_host + ":" + str(mongo_port) + "/" + mongo_db
    client = pymongo.MongoClient(mongo_connect)

    logging.debug("Init collections...")
    database = client[mongo_db]

    collection_orders = database["orders"]
    collection_users  = database["users"]
    orders = pd.read_csv("data/orders.csv")
    date_columns = ['created_at', 'date_tz', 'updated_at', 'fulfillment_date_tz']
    numeric_columns = ['item_count', 'subtotal', 'tax_percentage', 'total', 'total_discount', 'total_gratuity', 'total_tax']
    categoric_columns = ['receive_method', 'status']

    
    orders[date_columns] = orders[date_columns].fillna(datetime.datetime(1970,1,1))
    orders[date_columns] = orders[date_columns].apply(pd.to_datetime)
    orders[numeric_columns] = orders[numeric_columns].fillna(0)

    users  = pd.read_csv("data/users.csv")
    users[['created_at', 'updated_at']] = users[['created_at', 'updated_at']].fillna(datetime.datetime(1970,1,1)).apply(pd.to_datetime)

    logging.debug(orders.head())
    logging.debug(users.head())

    try:
        if collection_orders.count_documents({}) == 0:
            collection_orders.insert_many(orders.to_dict('records'))
        else:
            logging.info("Database has already been populated with orders")
        if collection_users.count_documents({}) == 0: 
            collection_users.insert_many(users.to_dict('records'))
        else:
            logging.info("Database has already been populated with users")
    except Exception as e:
        logging.error("Unable to insert the data: " + str(e))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    mongo_user = config.MONGO_USER
    mongo_pass = config.MONGO_PASSWORD
    mongo_host = config.MONGO_HOST
    mongo_port = config.MONGO_PORT
    mongo_db = config.MONGO_DB
    populateDB()