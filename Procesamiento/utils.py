
import pymysql
from datetime import datetime
import sshtunnel
import pandas as pd
from sqlalchemy import create_engine

sshtunnel.SSH_TIMEOUT = 30.0
sshtunnel.TUNNEL_TIMEOUT = 30.0

ssh_host = 'senialab.com'
ssh_user = 'senialab'
ssh_port = 22
ssh_password= 'senialab'

def create_server_ssh(_server,local_port):
    server = sshtunnel.SSHTunnelForwarder((ssh_host, ssh_port),ssh_username=ssh_user,ssh_password=ssh_password,remote_bind_address=(_server, 3306),local_bind_address=('localhost',local_port))
    return server

# nos conectamos a ls BD pasada como argumento
def connect(_server, _user, _password, _database):
    try:
        db = pymysql.connect(host=_server, user=_user, password=_password, database=_database)
        return db
    except pymysql.err.OperationalError as e:
        # e[0]
            # (1007, "Can't create database 'test_piloto'; database exists")
            # (1045, "Access denied for user 'jaime'@'localhost' (using password: YES)")
            # (1049, "Unknown database 'test_piloto'")
            # (2003, "Can't connect to MySQL server on 'localhost2' ([Errno -3] Temporary failure in name resolution)")
        print("Exeception occured in coonect:{}".format(e))
    except Exception as e:
        print("Unknown Exeception occured:{}\Please contact with admin.".format(e))


# nos desconectamos de la BD
def disconnect(_database):
    _database.close()


# crea la database
def create_database(_server, _user, _password, _database):
    sqlQuery = "CREATE DATABASE " + _database
    try:
        db = pymysql.connect(host=_server, user=_user, password=_password)
        cursor = db.cursor()
        cursor.execute(sqlQuery)
        disconnect(db)
    except Exception as e:
        print("Exeception occured in create:{}".format(e))

def create_server_shh(_server):
    server = sshtunnel.SSHTunnelForwarder((ssh_host, ssh_port),ssh_username=ssh_user,ssh_password=ssh_password,remote_bind_address=(_server, 3306))
    return server


# genera las 3 tablas correspondientes
def create_tables(_cursor):
    query = {}
    sqlQuery = """ CREATE TABLE sensor (
                id_sensor   varchar(255) NOT NULL,
                sensor      varchar(255) NOT NULL,
                description varchar(255),
                active      tinyint(1) DEFAULT 0,
                timestamp   timestamp DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id_sensor)
                )"""
    query['sensor'] = sqlQuery

    sqlQuery = """ CREATE TABLE location (
                id_location int NOT NULL AUTO_INCREMENT,
                id_sensor   varchar(255) NOT NULL,
                node        varchar(255),
                latitude    decimal(10,8),
                longitude   decimal(11,8),
                room        varchar(255),
                building    varchar(255),
                campus      varchar(255),
                PRIMARY KEY (id_location),
                FOREIGN KEY (id_sensor) REFERENCES sensor(id_sensor)
                )"""
    query['location'] = sqlQuery

    sqlQuery = """ CREATE TABLE data (
                id_data     int NOT NULL AUTO_INCREMENT,
                id_sensor   varchar(255) NOT NULL,
                timestamp   timestamp NOT NULL,
                variable    varchar(255) NOT NULL,
                value       float NOT NULL,
                unit        varchar(255) NOT NULL,
                PRIMARY KEY (id_data),
                FOREIGN KEY (id_sensor) REFERENCES sensor(id_sensor)
                )"""
    query['data'] = sqlQuery

    try:
        for key in query:
            print("creating table %s" %(key))
            _cursor.execute(query[key])
    except Exception as e:
        # (1046, 'No database selected')
        print("Exeception occured while creating table: {}".format(e))

def insert_measure_ssh(_id_sensor, _sensor_type, _measure, _unit, _timestamp, _server, _user, _password, _database):
    with sshtunnel.SSHTunnelForwarder((ssh_host, ssh_port),ssh_username=ssh_user,ssh_password=ssh_password,remote_bind_address=(_server, 3306)) as tunnel: 
        _db = pymysql.connect(host='127.0.0.1', user=_user,passwd=_password, db=_database,port=tunnel.local_bind_port)
        _cursor = _db.cursor()
        sql = """Insert INTO data (id_sensor,variable,value,unit,timestamp) VALUES ('%s', '%s', %s, '%s', '%s')""" %(_id_sensor,_sensor_type,_measure,_unit,_timestamp)
        try:
            _cursor.execute(sql)
            _db.commit()
        except Exception as e:
            # Rollback in case there is any error
            print(e)
            _db.rollback()
        _db.close()


def insert_sensor(_db, _cursor, _id_sensor, _sensor, _timestamp, _active):
    sqlQuery = """ INSERT INTO sensor (
                id_sensor,
                sensor,
                active,
                timestamp)
                VALUES ('%s', '%s', '%s', '%s') """ %(
                _id_sensor,
                _sensor,
                _active,
                _timestamp)

    try:
        _cursor.execute(sqlQuery)
        _db.commit()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()


def insert_location(_db, _cursor, _id_sensor, _node, _room, _building, _campus):
    sqlQuery = """ INSERT INTO location (
                id_sensor,
                node,
                room,
                building,
                campus)
                VALUES ('%s', '%s', '%s', '%s', '%s') """ %(
                _id_sensor,
                _node,
                _room,
                _building,
                _campus)

    try:
        _cursor.execute(sqlQuery)
        _db.commit()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()


# insertamos una medida
def insert_measure(_db, _cursor, _id_sensor, _sensor_type, _measure, _unit, _timestamp):
    sqlQuery = """Insert INTO data (
                id_sensor,
                variable,
                value,
                unit,
                timestamp)
                VALUES ('%s', '%s', %s, '%s', '%s')""" %(
                _id_sensor,
                _sensor_type,
                _measure,
                _unit,
                _timestamp)

    try:
        _cursor.execute(sqlQuery)
        _db.commit()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

# insertar alarmas

def insert_alarm(_db, _cursor, _id_sensor,_location, _measures, _treshold, _alarma, _epoch):
    sqlQuery = """Insert INTO alarm (
                id_sensor,
                campus,
                edificio,
                espacio,
                variable,
                value,
                unit,
                treshold,
                alarm,
                timestamp)
                VALUES ('%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s')""" %(
                _id_sensor,
                _location[2],
                _location[1],
                _location[0],
                _measures['name'],
                _measures['value'],
                _measures['unit'],
                _treshold,
                _alarma,
                _epoch)
    # print(sqlQuery)

    try:
        _cursor.execute(sqlQuery)
        _db.commit()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_sensor_alarm(_db, _cursor, _id_sensor, _measure):
    sqlQuery = """Select alarm,timestamp  from alarm where id_sensor = '%s' AND variable = '%s' ORDER BY TIMESTAMP DESC LIMIT 1""" %(_id_sensor, _measure)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchone()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_sensors(_db, _cursor, _campus):
    sqlQuery = """Select id_sensor from location where campus = '%s' ORDER BY id_location ASC""" %(_campus)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchall()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_sensors_all(_db, _cursor):
    sqlQuery = """Select id_sensor from location ORDER BY id_location ASC"""
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchall()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_sensors2(_db, _cursor, _id_sensor):
    sqlQuery = """Select sensor from sensor where id_sensor = '%s'""" %(_id_sensor)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchall()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_alarm_count(_db, _cursor, _id_sensor, _measure, _alarm):
    sqlQuery = """SELECT COUNT(alarm) FROM alarm WHERE alarm = '%s' AND id_sensor = '%s' AND variable = '%s' AND TIMESTAMP >= DATE_SUB(NOW(), INTERVAL 7 HOUR)""" %(_alarm, _id_sensor, _measure)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchone()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_alarm_timestamp(_db, _cursor, _id_sensor, _measure, _alarm):
    sqlQuery = """SELECT timestamp FROM alarm WHERE alarm = '%s' AND id_sensor = '%s' AND variable = '%s' AND TIMESTAMP >= DATE_SUB(NOW(), INTERVAL 7 HOUR)""" %(_alarm, _id_sensor, _measure)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchall()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

# para verificar que llegan datos correctamente
def get_data_count(_db, _cursor, _id_sensor, _variable):
    sqlQuery = """SELECT COUNT(value) FROM data WHERE id_sensor = '%s' AND variable = '%s' AND TIMESTAMP >= DATE_SUB(NOW(), INTERVAL 1 HOUR)""" %( _id_sensor, _variable)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchone()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

# para el ultimo dato
def get_data_last(_db, _cursor, _id_sensor, _variable):
    sqlQuery = """SELECT value FROM data WHERE id_sensor = '%s' AND variable = '%s' ORDER BY TIMESTAMP DESC LIMIT 1""" %( _id_sensor, _variable)
    # print(sqlQuery)
    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchone()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_sensor(_db, _cursor, _id_sensor):
    sqlQuery = """Select * from sensor where id_sensor = '%s'""" %(_id_sensor)

    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchone()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_location(_db, _cursor, _id_sensor):
    sqlQuery = """Select room,building,campus from location where id_sensor = '%s'"""%(_id_sensor)

    try:
        _cursor.execute(sqlQuery)
        return _cursor.fetchone()
    except Exception as e:
        # Rollback in case there is any error
        print(e)
        _db.rollback()

def get_data_ssh(
    query,
    ssh_host,
    ssh_port,
    ssh_user,
    ssh_password,
    db_server,
    db_user,
    db_password,
    database
    ):
    with sshtunnel.SSHTunnelForwarder((ssh_host, ssh_port),
                                      ssh_username=ssh_user,
                                      ssh_password=ssh_password,
                                      remote_bind_address=(db_server, 3306)) as tunnel:
        _db = pymysql.connect(host='127.0.0.1',
                              user=db_user,
                              passwd=db_password,
                              db=database,
                              port=tunnel.local_bind_port)
        _cursor = _db.cursor()
        try:
            data = pd.read_sql(query, _db)
            return data
        except Exception as e:
            # Rollback in case there is any error
            print(e)
            _db.rollback()
        _db.close()


def write_data_ssh(
    dataframe,
    table_name,
    ssh_host,
    ssh_port,
    ssh_user,
    ssh_password,
    db_server,
    db_user,
    db_password,
    database,
    if_exists='fail'
    ):
    with sshtunnel.SSHTunnelForwarder((ssh_host, ssh_port),
                                      ssh_username=ssh_user,
                                      ssh_password=ssh_password,
                                      remote_bind_address=(db_server, 3306)) as tunnel:
        _db = pymysql.connect(host='127.0.0.1', user=db_user, passwd=db_password, db=database, port=tunnel.local_bind_port)
        try:
            connection_string = f"mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{database}"
            engine = create_engine(connection_string)

            dataframe.to_sql(table_name, engine, if_exists=if_exists, index=False)

        except Exception as e:
            print(e)
            _db.rollback()

        _db.close()