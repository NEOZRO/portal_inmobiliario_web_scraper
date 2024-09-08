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

def get_master_ids(conn,main):
    """ check if master if already exist in db , if not it creates it """
    cur = conn.cursor()
    cur.execute('SELECT ID FROM master_id WHERE latitude=? AND longitude=? AND titulo=?', main)
    result = cur.fetchone()

    # If exists, get the master_id
    if result:
        master_id = result[0]

    else:
        # If not exists, insert new record and get the master_id
        sql_insert_masterid = ''' INSERT INTO master_id(latitude, longitude, titulo)
                                      VALUES(?, ?, ?) '''
        cur.execute(sql_insert_masterid, main)
        master_id = cur.lastrowid

    print("master_id: ", master_id)
    return master_id
def insert_or_update_property(conn,main,values):
    """
    Inserts property data into the database
    :param conn: conection to database
    :param main: main values of property [lat,long,title]
    :param values: rest of the values
    """

    master_id = get_master_ids(conn,main)

    sql = ''' INSERT INTO properties (ID,
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
                                      gastos_comunes, 
                                      link, 
                                      geo_ref_name, 
                                      listed)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT (ID) DO UPDATE SET
                  dias_desde_publicacion = EXCLUDED.dias_desde_publicacion,
                  link = EXCLUDED.link,
                  listed = EXCLUDED.listed

          '''
    cur = conn.cursor()
    cur.execute(sql, (master_id,) + values)
    conn.commit()


def insert_price_history(conn,main, price, Price_UF,tipo_operacion,date):
    """Inserts price history data into the database"""

    master_id = get_master_ids(conn,main)

    sql = ''' INSERT INTO price_history(ID, Price, Price_UF,tipo_operacion, Date)
              VALUES(?,?,?,?,?) 
              '''
    cur = conn.cursor()
    cur.execute(sql, (master_id, price, Price_UF,tipo_operacion, date))
    conn.commit()

def get_joined_data_as_dataframe(conn,threshold_date): # todo: FIX
    """
    Fetches the join of properties and price_history tables from the database
    and returns it as a pandas DataFrame.
    """
    join_query = f"""
    SELECT properties.*, price_history.Price, price_history.Price_UF, price_history.tipo_operacion, price_history.Date
    FROM properties
    JOIN price_history ON properties.Latitude = price_history.Latitude AND properties.Longitude = price_history.Longitude AND properties.titulo = price_history.titulo
    WHERE price_history.Date > '{threshold_date}'
    """

    try:
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

        sql_create_unique_id = """ CREATE TABLE IF NOT EXISTS master_id (
                                            ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                            latitude REAL NOT NULL,
                                            longitude REAL NOT NULL,
                                            titulo TEXT NOT NULL); """

        sql_create_properties_table = """ CREATE TABLE IF NOT EXISTS properties (
                                            ID INTEGER PRIMARY KEY, 
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
                                            gastos_comunes REAL,
                                            link TEXT NOT NULL,
                                            geo_ref_name TEXT NOT NULL,
                                            listed BOOLEAN NOT NULL,
                                            FOREIGN KEY (ID) REFERENCES master_id (ID)
                                        ); """

        sql_create_price_history_table = """ CREATE TABLE IF NOT EXISTS price_history (
                                            PriceID INTEGER PRIMARY KEY AUTOINCREMENT,
                                            ID INTEGER, 
                                            Price REAL NOT NULL,
                                            Price_UF REAL NOT NULL,
                                            Date TEXT NOT NULL,
                                            tipo_operacion TEXT NOT NULL,
                                            FOREIGN KEY (ID) REFERENCES master_id (ID)
                                        );"""

        sql_create_logs_table = """ CREATE TABLE IF NOT EXISTS logs (
                                            LogID INTEGER PRIMARY KEY AUTOINCREMENT,
                                            geo_ref_name TEXT NOT NULL,
                                            Date TEXT NOT NULL,
                                            link TEXT NOT NULL,
                                            exception_print TEXT NOT NULL,
                                            solved_status BOOLEAN NOT NULL
                                            );"""

        create_table(conn, sql_create_unique_id)
        create_table(conn, sql_create_properties_table)
        create_table(conn, sql_create_price_history_table)
        create_table(conn, sql_create_logs_table)

    return conn

def check_db_exists(db_filename):
    """ check if db file already exist"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, db_filename)
    return os.path.isfile(db_path)



