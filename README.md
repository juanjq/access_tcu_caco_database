# access_tcu_caco_database

## 0. Access IT cluster
To simplify the process you need to add it in `.ssh/config`, for this case I'm using `lst101`.

## 1. Connect to the database
To open the connection to the databases you need to access `lst101` with the two specific ports:
* For **CaCo** the port is `27018`
```
ssh lst101 -L27018:127.0.0.1:27018
```
* For **TCU** the port is `27017`
```
ssh lst101 -L27017:127.0.0.1:27017
```
These two connections can be done automatically launching `connect_databases.sh`

The connections need to be open all the time you want to access the database.

## 2. Check the properties available in each database
