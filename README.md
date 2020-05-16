# mongo2postgre

Migrate collections named "orders" and "users" (with structure presented in data/orders.csv and data/users.csv files) from MongoDB to one PostgreSQL table.

## Installation

Clone git repository
```
git clone https://github.com/IRailean/GoParrot_Test_Task.git
```
go to project directory and run
```
pip install -e .
```

## Usage
### Populate MongoDB with the data located in data folder:
Run
```
python app/mongoDB.py
```
Parameters:
```
MongoDB connection
-muser - user
-mpass - password
-mhost - host
-mport - port     
-mdb   - database name
```
defaults can be found in app/config.py

### Migrate MongoDB to PostgreSQL
Run
```
python app/app.py
```
Starting with 01-01-2020 every 5 minutes MongoDB data will be updated and inserted into "data" table in PostgreSQL.
Parameters:
```
MongoDB connection
-muser - user
-mpass - password
-mhost - host
-mport - port     
-mdb   - database name

PostgreSQL connection
-pguser - user
-pgpass - password
-pghost - host
-pgport - port     
-pgdb   - database name
```

## Enjoy!
