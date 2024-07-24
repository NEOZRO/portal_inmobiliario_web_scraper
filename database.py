from datetime import datetime
import sqlite3
import pandas as pd


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

def insert_property(conn, property):
    sql = ''' INSERT INTO properties(Latitude, Longitude ,
                                        dias_desde_publicacion ,
                                        n_dormitorios ,
                                        n_banos ,
                                        superficie_total ,
                                        superficie_util ,
                                        estacionamientos ,
                                        bodegas ,
                                        antiguedad ,
                                        cantidad_pisos_edificio ,
                                        piso_unidad ,
                                        tipo_inmueble ,
                                        orientacion ,
                                        titulo ,
                                        ubicacion ,
                                        link ,
                                        geo_ref_name)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
              '''
    cur = conn.cursor()
    cur.execute(sql, property)
    conn.commit()

def insert_price_history(conn, latitude, longitude, price,Price_UF,tipo_operacion, date):
    sql = ''' INSERT INTO price_history(Latitude, Longitude, Price, Price_UF,tipo_operacion, Date)
              VALUES(?,?,?,?,?,?) 
              '''
    cur = conn.cursor()
    cur.execute(sql, (latitude, longitude, price, Price_UF,tipo_operacion, date))
    conn.commit()

def get_price_history(conn, latitude, longitude):
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
    # SQL query to join properties and price_history tables
    # join_query = """
    # SELECT *
    # FROM properties
    # JOIN price_history ON properties.Latitude = price_history.Latitude AND properties.Longitude = price_history.Longitude
    # """

    # Adjusted SQL query to include the date condition
    join_query = f"""
    SELECT properties.*, price_history.Price, price_history.Price_UF, price_history.tipo_operacion, price_history.Date
    FROM properties
    JOIN price_history ON properties.Latitude = price_history.Latitude AND properties.Longitude = price_history.Longitude
    WHERE price_history.Date >= '{threshold_date}'
    """


    try:
        # Use pandas to read the query result directly into a DataFrame
        df = pd.read_sql_query(join_query, conn)
        return df
    except Exception as e:
        print(e)
        return None
    finally:
        # Ensure the connection is closed after the operation
        conn.close()



def main():
    database = "real_estate.db"

    sql_create_properties_table = """ CREATE TABLE IF NOT EXISTS properties (
                                        Latitude REAL NOT NULL,
                                        Longitude REAL NOT NULL,
                                        dias_desde_publicacion REAL NOT NULL,
                                        n_dormitorios REAL NOT NULL,
                                        n_banos REAL NOT NULL,
                                        superficie_total REAL NOT NULL,
                                        superficie_util REAL NOT NULL,
                                        estacionamientos REAL NOT NULL,
                                        bodegas REAL NOT NULL,
                                        antiguedad REAL NOT NULL,
                                        cantidad_pisos_edificio REAL NOT NULL,
                                        piso_unidad REAL NOT NULL,
                                        tipo_inmueble TEXT NOT NULL,
                                        orientacion TEXT NOT NULL,
                                        titulo TEXT NOT NULL,
                                        ubicacion TEXT NOT NULL,
                                        link TEXT NOT NULL,
                                        geo_ref_name TEXT NOT NULL,
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

    # Create a database connection
    conn = create_db_connection(database)

    # Create tables
    if conn is not None:
        create_table(conn, sql_create_properties_table)
        create_table(conn, sql_create_price_history_table)
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':

    # main()

    # inserting values
    database = "real_estate_data.db"
    conn = create_db_connection(database)
    insert_price_history(conn, 50, -50, 999, datetime.now())

    pass

# TODO:
#    1. INSERTAR PROPIEDADES DE CADA REAL STATE
#     2. RECUPERAR EL JOINED CON EL UKLTIMO PRECIO DE CADA PROPIEDAD
#     3. UPDATEAR PROPIEDADES
#     4. GET JOINED TABLE WITH
#  5. AGREGAR TABLA
#%%
