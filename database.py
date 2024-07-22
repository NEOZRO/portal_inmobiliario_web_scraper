from datetime import datetime
import sqlite3


def create_connection(db_file):
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
    sql = ''' INSERT INTO properties(Latitude, Longitude, Address, NumberOfBedrooms, Size, Type, OtherStaticInfo)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, property)
    conn.commit()
    return (property[0], property[1])  # Return latitude and longitude as a unique identifier

def insert_price_history(conn, latitude, longitude, price, date):
    sql = ''' INSERT INTO price_history(Latitude, Longitude, Price, Date)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (latitude, longitude, price, date))
    conn.commit()

def get_price_history(conn, latitude, longitude):
    sql = "SELECT * FROM price_history WHERE Latitude=? AND Longitude=?"
    cur = conn.cursor()
    cur.execute(sql, (latitude, longitude))
    rows = cur.fetchall()
    for row in rows:
        print(row)


def main():
    database = "real_estate_data.db"

    sql_create_properties_table = """ CREATE TABLE IF NOT EXISTS properties (
                                        Latitude REAL NOT NULL,
                                        Longitude REAL NOT NULL,
                                        Price REAL NOT NULL,
                                        Price_UF REAL NOT NULL,
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
                                        PRIMARY KEY (Latitude, Longitude)
                                    ); """

    sql_create_price_history_table = """ CREATE TABLE IF NOT EXISTS price_history (
                                        PriceID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Latitude REAL NOT NULL,
                                        Longitude REAL NOT NULL,
                                        Price REAL NOT NULL,
                                        Date TEXT NOT NULL,
                                        FOREIGN KEY (Latitude, Longitude) REFERENCES properties (Latitude, Longitude)
                                    );"""

    # Create a database connection
    conn = create_connection(database)

    # Create tables
    if conn is not None:
        create_table(conn, sql_create_properties_table)
        create_table(conn, sql_create_price_history_table)
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':

    # main()

    # # inserting values
    # database = "real_estate_data.db"
    # conn = create_connection(database)
    # insert_price_history(conn, 50, -50, 999, datetime.now())

    pass

# TODO:
   1. INSERTAR PROPIEDADES DE CADA REAL STATE
    2. RECUPERAR EL JOINED CON EL UKLTIMO PRECIO DE CADA PROPIEDAD
    3. UPDATEAR PROPIEDADES
    4. GET JOINED TABLE WITH
 5. AGREGAR TABLA