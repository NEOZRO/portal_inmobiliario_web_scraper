from datetime import datetime
import sqlite3
import pandas as pd
import os

def create_db_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """Create a table from the create_table_sql statement"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)

def insert_or_update_property(conn, property):
    """Inserts property data into the database"""

    sql = ''' INSERT INTO properties (Latitude, Longitude, 
                                      dias_desde_publicacion, 
                                      n_dormitorios, 
                                      n_banos, 
                                      superficie_total, 
                                      superficie_util, 
                                      estacionamientos, 
                                      bodegas, 
                                      antiguedad, 
                                      cantidad_pisos_edificio, 
                                      piso_unidad, 
                                      tipo_inmueble, 
                                      orientacion, 
                                      titulo, 
                                      ubicacion, 
                                      link, 
                                      geo_ref_name, 
                                      listed)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT (Latitude, Longitude) DO UPDATE SET
                  dias_desde_publicacion = EXCLUDED.dias_desde_publicacion,
                  link = EXCLUDED.link,
                  listed = EXCLUDED.listed

          '''
    cur = conn.cursor()
    cur.execute(sql, property)
    conn.commit()

def insert_price_history(conn, latitude, longitude, price,Price_UF,tipo_operacion, date):
    """Inserts price history data into the database"""
    sql = ''' INSERT INTO price_history(Latitude, Longitude, Price, Price_UF,tipo_operacion, Date)
              VALUES(?,?,?,?,?,?) 
              '''
    cur = conn.cursor()
    cur.execute(sql, (latitude, longitude, price, Price_UF,tipo_operacion, date))
    conn.commit()

def get_price_history(conn, latitude, longitude):
    """Fetches price history data from the database"""
    sql = "SELECT * FROM price_history WHERE Latitude=? AND Longitude=?"
    cur = conn.cursor()
    cur.execute(sql, (latitude, longitude))
    rows = cur.fetchall()
    for row in rows:
        print(row)

def get_joined_data_as_dataframe(conn,threshold_date):
    """
    Fetches the join of properties and price_history tables from the database
    and returns it as a pandas DataFrame.
    """
    join_query = f"""
    SELECT properties.*, price_history.Price, price_history.Price_UF, price_history.tipo_operacion, price_history.Date
    FROM properties
    JOIN price_history ON properties.Latitude = price_history.Latitude AND properties.Longitude = price_history.Longitude
    WHERE price_history.Date > '{threshold_date}'
    """


    try:
        # Use pandas to read the query result directly into a DataFrame
        df = pd.read_sql_query(join_query, conn)
        return df
    except Exception as e:
        print(e)
        return None
    finally:
        conn.close()

def delist_all_properties(conn):
    """ mark all properties as not listed"""

    sql_query = f"""
    UPDATE properties
     SET listed = false;
    """
    cur = conn.cursor()
    cur.execute(sql_query)
    conn.commit()


def insert_error_log(conn, geo_ref_name, Date,link,exception_print, solved_status):
    """ isert a log error into the log table"""

    sql = ''' INSERT INTO logs(geo_ref_name, Date,link,exception_print, solved_status)
              VALUES(?,?,?,?,?) 
              '''
    cur = conn.cursor()
    cur.execute(sql, (geo_ref_name, Date,link,exception_print, solved_status))
    conn.commit()


def create_conect_db(name):
    """ create the db (if doesn't exist) and return the connection"""

    conn = create_db_connection(name)

    if check_db_exists:

        sql_create_properties_table = """ CREATE TABLE IF NOT EXISTS properties (
                                            Latitude REAL NOT NULL,
                                            Longitude REAL NOT NULL,
                                            dias_desde_publicacion REAL NOT NULL,
                                            n_dormitorios REAL ,
                                            n_banos REAL ,
                                            superficie_total REAL ,
                                            superficie_util REAL ,
                                            estacionamientos REAL ,
                                            bodegas REAL ,
                                            antiguedad REAL ,
                                            cantidad_pisos_edificio REAL ,
                                            piso_unidad REAL ,
                                            tipo_inmueble TEXT ,
                                            orientacion TEXT ,
                                            titulo TEXT ,
                                            ubicacion TEXT ,
                                            link TEXT NOT NULL,
                                            geo_ref_name TEXT NOT NULL,
                                            listed BOOLEAN NOT NULL,
                                            PRIMARY KEY (Latitude, Longitude)
                                        ); """

        sql_create_price_history_table = """ CREATE TABLE IF NOT EXISTS price_history (
                                            PriceID INTEGER PRIMARY KEY AUTOINCREMENT,
                                            Latitude REAL NOT NULL,
                                            Longitude REAL NOT NULL,
                                            Price REAL NOT NULL,
                                            Price_UF REAL NOT NULL,
                                            Date TEXT NOT NULL,
                                            tipo_operacion TEXT NOT NULL,
                                            FOREIGN KEY (Latitude, Longitude) REFERENCES properties (Latitude, Longitude)
                                        );"""

        sql_create_logs_table = """ CREATE TABLE IF NOT EXISTS logs (
                                            LogID INTEGER PRIMARY KEY AUTOINCREMENT,
                                            geo_ref_name TEXT NOT NULL,
                                            Date TEXT NOT NULL,
                                            link TEXT NOT NULL,
                                            exception_print TEXT NOT NULL,
                                            solved_status BOOLEAN NOT NULL
                                            );"""

        create_table(conn, sql_create_properties_table)
        create_table(conn, sql_create_price_history_table)
        create_table(conn, sql_create_logs_table)

    return conn

def check_db_exists(db_filename):
    """ check if db file already exist"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, db_filename)
    return os.path.isfile(db_path)



