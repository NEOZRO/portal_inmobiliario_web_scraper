import sqlite3
import pandas as pd
import os
import ast


class DatabaseManager:
    """ class to handle database """
    def __init__(self):

        self.database_name = None
        self.conn = None
        # self.database_name = "real_state.db"
        # self.create_db_connection(self.database_name)

    def create_db_connection(self,db_file):
        """Create a database connection to the SQLite database specified by db_file"""
        try:
            conn = sqlite3.connect(db_file)
            self.conn = conn
            return conn
        except Exception as e:
            print(e)


    def create_table(self, create_table_sql):
        """Create a table from the create_table_sql statement"""
        try:
            c = self.conn.cursor()
            c.execute(create_table_sql)
        except Exception as e:
            print(e)

    def get_master_ids(self,main):
        """ check if master if already exist in db , if not it creates it """
        cur = self.conn.cursor()
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

        return master_id

    def get_map_ids(self,geo_ref_name):
        """ check if map id is already exist in db , if not it creates it """
        cur = self.conn.cursor()
        cur.execute('''SELECT mapID FROM maps_id WHERE geo_ref_name=?''', (geo_ref_name,))
        result = cur.fetchone()

        # If exists, get the map_id
        if result:
            map_id = result[0]

        else:
            # If not exists, insert new record and get the master_id
            sql_insert_masterid = '''INSERT INTO maps_id(geo_ref_name)
                                        VALUES(?)'''
            cur.execute(sql_insert_masterid, (geo_ref_name,))
            map_id = cur.lastrowid

        return map_id

    def insert_or_update_property(self,main,values):
        """
        Inserts property data into the database
        :param self.conn: conection to database
        :param main: main values of property [lat,long,title]
        :param values: rest of the values
        """
        self.create_conect_db(self.database_name)
        master_id = self.get_master_ids(main)

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
                                          mapID, 
                                          listed)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                  ON CONFLICT (ID) DO UPDATE SET
                      dias_desde_publicacion = EXCLUDED.dias_desde_publicacion,
                      mapID = EXCLUDED.mapID,
                      link = EXCLUDED.link,
                      listed = EXCLUDED.listed
    
              '''
        cur = self.conn.cursor()
        cur.execute(sql, (master_id,) + values)
        self.conn.commit()
        self.conn.close()



    def insert_price_history(self,main, price, Price_UF,tipo_operacion,date):
        """Inserts price history data into the database"""

        self.create_conect_db(self.database_name)
        master_id = self.get_master_ids(main)

        sql = ''' INSERT INTO price_history(ID, Price, Price_UF,tipo_operacion, Date)
                  VALUES(?,?,?,?,?) 
                  '''
        cur = self.conn.cursor()
        cur.execute(sql, (master_id, price, Price_UF,tipo_operacion, date))
        self.conn.commit()

    def get_joined_data_as_dataframe(self,threshold_date): # todo: FIX
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
            df = pd.read_sql_query(join_query, self.conn)
            return df
        except Exception as e:
            print(e)
            return None
        finally:
            self.conn.close()

    def delist_all_properties(self):
        """ mark all properties as not listed"""
        self.create_conect_db(self.database_name)
        sql_query = f"""
        UPDATE properties
         SET listed = false;
        """
        cur = self.conn.cursor()
        cur.execute(sql_query)
        self.conn.commit()
        self.conn.close()


    def insert_error_log(self, map_id, Date,link,exception_print, solved_status):
        """ isert a log error into the log table"""

        self.create_conect_db(self.database_name)
        sql = ''' INSERT INTO logs(mapID, Date,link,exception_print, solved_status)
                  VALUES(?,?,?,?,?) 
                  '''
        cur = self.conn.cursor()
        cur.execute(sql, (map_id, Date,link,exception_print, solved_status))
        self.conn.commit()
        self.conn.close()

    def insert_new_cluser_map(self, geo_ref_name,geojson_data):
        """ isert a new cluster of geo fences into the maps table db"""
        self.create_conect_db(self.database_name)
        map_id = self.get_map_ids(geo_ref_name)

        sql = ''' INSERT INTO maps(mapID, geojson_data)
                  VALUES(?,?)
                  ON CONFLICT (mapID) DO UPDATE SET
                      geojson_data = EXCLUDED.geojson_data
                  '''
        cur = self.conn.cursor()
        if geojson_data is None:
            geojson_data = str([])
        cur.execute(sql, (map_id, geojson_data))
        self.conn.commit()
        self.conn.close()


    def list_db_maps(self):
        """ list all avaliable cluster maps in the maps table db"""
        self.create_conect_db(self.database_name)
        join_query = f"""
                        SELECT
                            maps_id.mapID,
                            maps_id.geo_ref_name,
                            maps.geojson_data
                        FROM
                            maps
                        INNER JOIN
                            maps_id
                        ON
                            maps.mapID = maps_id.mapID;
        """
        try:
            df = pd.read_sql_query(join_query, self.conn)
            df = df.T.groupby(level=0).first().T # remove duplicated columns, based on first
            print(df[['mapID', 'geo_ref_name']])
            self.conn.close()
        except Exception as e:
            print(e)
            self.conn.close()
            return None



    def get_maps_data(self,map_id):
        """ list all avaliable cluster maps in the maps table db"""
        self.create_conect_db(self.database_name)
        df=None
        join_query = f"""
                        SELECT
                            maps_id.mapID,
                            maps_id.geo_ref_name,
                            maps.geojson_data,
                            maps.mapID
                        FROM
                            maps
                        INNER JOIN
                            maps_id
                        ON
                            maps.mapID = {map_id};
        """
        try:
            df = pd.read_sql_query(join_query, self.conn)
            self.conn.close()
        except Exception as e:
            print(e)

        finally:
            self.conn.close()
            df = df.T.groupby(level=0).first().T # remove duplicated columns, based on first
            return df


    def create_conect_db(self, name):
        """ create the db (if doesn't exist) and return the connection"""

        self.create_db_connection(name)

        if self.check_db_exists:

            sql_create_unique_id = """ CREATE TABLE IF NOT EXISTS master_id (
                                                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                latitude REAL NOT NULL,
                                                longitude REAL NOT NULL,
                                                titulo TEXT NOT NULL); """

            sql_create_maps_id = """ CREATE TABLE IF NOT EXISTS maps_id (
                                                mapID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                geo_ref_name TEXT NOT NULL
                                                );"""

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
                                                mapID INTEGER NOT NULL,
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
                                                mapID INTEGER NOT NULL,
                                                Date TEXT NOT NULL,
                                                link TEXT NOT NULL,
                                                exception_print TEXT NOT NULL,
                                                solved_status BOOLEAN NOT NULL,
                                                FOREIGN KEY (mapID) REFERENCES maps_id (mapID)
                                                );"""

            sql_create_maps_table = """ CREATE TABLE IF NOT EXISTS maps (
                                                mapID INTEGER PRIMARY KEY,
                                                geojson_data TEXT NOT NULL,
                                                FOREIGN KEY (mapID) REFERENCES maps_id (mapID)
                                                );"""


            self.create_table( sql_create_unique_id)
            self.create_table( sql_create_maps_id)
            self.create_table( sql_create_properties_table)
            self.create_table( sql_create_price_history_table)
            self.create_table( sql_create_maps_table)
            self.create_table( sql_create_logs_table)


        return self.conn


    def check_db_exists(self, db_filename):
        """ check if db file already exist"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(current_dir, db_filename)
        return os.path.isfile(db_path)

