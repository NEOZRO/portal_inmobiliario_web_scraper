

from time import sleep, time
import numpy as np
from logs import log_exception_str
from datetime import datetime
import ssl
import os
import pandas as pd
import ast
from ipyleaflet import DrawControl
from ipyleaflet import Map, TileLayer, DrawControl, GeoJSON

from database import DatabaseManager
from utils import WebDriver,ExchangeVariables,DataExtractor,InteractiveMap,ProgressBar,Analytics

class WebScraperPortalInmobiliario(WebDriver,ExchangeVariables,DataExtractor,InteractiveMap,ProgressBar,DatabaseManager,Analytics):
    """
    Created by Brian Lavin
    https://www.linkedin.com/in/brian-lavin/
    ---
    webscrappoing data from portal inmobiliario
    Works drawing a POLYGON into the map
    https://www.portalinmobiliario.com/
    :param tipo_inmueble: select one -> ["casa","departamento"]
    :param theme: select one -> ["dark","default","white"]
    :return: df_results, xlsx file (optional)
    """

    def __init__(self):
        super().__init__()

        self.conn_map = None
        self.list_geo_urls_pages = []
        self.usd_clp = None
        self.table_data = None
        self.wea_soup = []
        self.save_soup_text = []
        self.wea = []
        self.longitud = []
        self.latitud = []
        self.dias_desde_publicacion = []
        self.piso_unidad = []
        self.GC = []
        self.antiguedad = []
        self.orientacion = []
        self.tipo_inmueble = []
        self.cantidad_pisos = []
        self.bodegas = []
        self.estacionamientos = []
        self.superficie_util = []
        self.superficie_total = []
        self.metraje_total = []
        self.metraje_util = []
        self.ubicacion = []
        self.title = []
        self.price_symbol = []
        self.precios = []
        self.n_banos = []
        self.n_dormitorios = []
        self.metraje = []
        self.cards_urls = []
        self.list_operations = []
        self.list_tipos_inmueble = []
        self.error_msg = []  #   DEBUG
        self.error_soups = []  #  DEBUG
        self.error_links = []  #  DEBUG

        self.actions_list = []
        self.total_dict_properties = None
        self.main_soup = None
        self.uf = None
        self.region = None
        self.n_pages = None
        self.main_interactive_map = None
        self.geo_url_main = None
        self.total_number_of_properties = None
        self.df_results = None
        self.driver = None
        self.df_inversion_venta = None
        self.df_inversion_arriendo = None
        self.full_path = None
        self.current_url = None
        self.last_wea = None
        # keep this line only if you trust your proxy provider
        ssl._create_default_https_context = ssl._create_unverified_context # NOQA.

        self.database_name = "real_state.db"
        self.conn = self.create_conect_db(self.database_name)  # it creates only if it doesn't exist

        self.current_time_str = datetime.now().strftime('%H%M_%d_%m_%y')
        self.theme = "default"

        self.tipo_operacion = None
        self.picked_pts_features = []

        self.get_uf_today()
        self.get_today_USD_CLP_value()
        self.total_number_request = 0

        self.analysis_results={}


    def start_download(self,selected_mapID):
        """ if is already a folder with a poly selection. SKIP poly selection and just download the new data of the location """

        self.empty_lists_except_specific(self.picked_pts_features)
        self.init_webdriver(get_images=True)
        self.init_progress_bar(max_len = 100)
        self.selected_map_id = selected_mapID

        try:
            self.load_geojson_data()
            self.bar_set(f"Loading db...")
            self.execute_main_process()

        except Exception as e:
            self.bar_set("Error during main process")
            print(e)


    def vis_map(self,selected_mapID):
        """
        Execute main map visualization and load the selected map group. allows to create and delete polygons
        :param selected_mapID:
        :return:
        """
        df = self.get_maps_data(selected_mapID)
        if len(df) > 0:
            geojson_data = df.query(f"mapID == {selected_mapID}")["geojson_data"].values[0]
            json_data_selected = ast.literal_eval(geojson_data)
            self.folder_save_name = df.query(f"mapID == {selected_mapID}")["geo_ref_name"].values[0]
            self.picked_pts_features = json_data_selected
            print(f"{self.folder_save_name} --> Sucess!")
        else:
            raise ValueError("No maps found for that mapID")

        main_interactive_map = self.init_map_ipyflet(theme=self.theme,
                                                     center_map_cordinates=self.get_geofences_center_coords(), # make center dinamic
                                                     geojson_data=None,
                                                     zoom=14)

        draw_control = DrawControl(
            rectangle={},
            circlemarker={},
            polyline={})

        draw_control.on_draw(self.handle_draw)
        main_interactive_map.add(draw_control)


        draw_control.clear_polygons()
        draw_control.clear()
        draw_control.data = self.picked_pts_features

        display(main_interactive_map)  # NOQA

    def empty_lists_except_specific(self, keep):
        """ clear all lists in the class to store new data, except the one to keep [polygon coordinates]"""
        for attr in self.__dict__:
            if isinstance(getattr(self, attr), list) and attr != keep:
                setattr(self, attr, [])


    def get_data_from_urls(self):
        """
        iterate over all the urls found in the location for each house/real state
        Saves the data into lists to later make a dataframe
        """
        self.bar_set(f" Obtaining data...[starting]")
        self.close_webdriver()
        self.init_webdriver(get_images=False)

        for i, url in enumerate(self.cards_urls):

            operacion = self.list_operations[i]
            tipo = self.list_tipos_inmueble[i]
            title, latitud, longitud, price_value, n_days_since_published = self.init_main_properties()
            self.init_dict_properties()

            soup = None
            try:
                self.bar.update(1)
                self.current_url = url

                init_time = time()

                self.bar_set(f'{operacion}/{tipo}: Obtaining data...[get right soup]')
                soup = self.get_correct_soup_from_url(url)

                self.bar_set(f'{operacion}/{tipo}: Obtaining data...[data from table]')
                self.get_data_from_table_in_url()

                self.bar_set(f'{operacion}/{tipo}: Obtaining data...[extracting features]')
                title, latitud, longitud, price_value, n_days_since_published = self.get_main_properties_from_soup(soup)

                self.bar_set(f'{operacion}/{tipo}: Obtaining data...[saving results]')
                self.append_all_propierties(title,latitud,longitud,price_value,n_days_since_published)

                end_time = time()

                if self.total_number_request >= 100 or end_time - init_time > 20:
                    self.bar_set(f'{operacion}/{tipo}: Obtaining data...[Changing proxy]')
                    self.ip_status_index += 1
                    self.close_webdriver()
                    sleep(5)
                    self.init_webdriver(get_images=False)

            except Exception as e:
                self.insert_error_log(
                                 self.folder_save_name,
                                 datetime.now(),
                                 self.current_url,
                                 log_exception_str(exception=e),
                                 False)

                self.error_msg.append(log_exception_str(exception=e))
                self.error_links.append(self.current_url)
                self.error_soups.append(soup)

                # save in global list the partial results before the error, errors and later become nan
                self.append_all_propierties(title,latitud,longitud,price_value,n_days_since_published)

                end_time = time()
                if self.total_number_request >= 100 or end_time - init_time > 20: # NOQA
                    self.bar_set(f'{operacion}/{tipo}: Obtaining data...[Changing proxy]')
                    self.ip_status_index += 1
                    self.close_webdriver()
                    sleep(5)
                    self.init_webdriver(get_images=False)

                continue


    def compile_results_df(self):
        """ generate the final dataframe of results"""
        dict_df = {
            "latitud": self.latitud,
            "longitud": self.longitud,
            "precio": self.precios,
            "precio_UF": np.round(np.asarray(self.precios) / self.uf,2),
            "dias_desde_publicacion": self.dias_desde_publicacion,
            "n_dormitorios": self.n_dormitorios,
            "n_banos": self.n_banos,
            "superficie_total": self.superficie_total,
            "superficie_util": self.superficie_util,
            "estacionamientos": self.estacionamientos,
            "bodegas": self.bodegas,
            "antiguedad": self.antiguedad,
            "cantidad_pisos_edificio": self.cantidad_pisos,
            "piso_unidad": self.piso_unidad,
            "tipo_inmueble": self.list_tipos_inmueble,
            "orientacion": self.orientacion,
            "gastos_comunes": self.GC,
            "titulo": self.title,
            "link": self.cards_urls}

        self.df_results = pd.DataFrame(dict_df)

    def compile_results_df_to_db(self):
        """ adds the results from df to the database"""

        for i, row in self.df_results.iterrows():
            self.insert_or_update_property(
                                      (row.latitud,row.longitud,row.titulo),
                                      (row.dias_desde_publicacion,
                                             row.n_dormitorios,
                                             row.n_banos,
                                             row.superficie_total,
                                             row.superficie_util,
                                             row.estacionamientos,
                                             row.bodegas,
                                             row.antiguedad,
                                             row.cantidad_pisos_edificio,
                                             row.piso_unidad,
                                             row.tipo_inmueble,
                                             row.orientacion,
                                             row.gastos_comunes,
                                             row.link,
                                             self.selected_map_id,
                                             True))
            self.insert_price_history(
                                 (row.latitud,row.longitud,row.titulo),
                                 self.selected_map_id,
                                 row.precio,
                                 row.precio_UF,
                                 self.list_operations[i],
                                 datetime.now())


    def create_results_folder(self):
        """ create folder for results """
        if not os.path.exists("results"):
            os.makedirs("results")

        self.full_path = os.path.join("results", self.folder_save_name)
        if not os.path.exists(self.full_path):
            os.makedirs(self.full_path)


    def execute_main_process(self):
        """ main process"""

        self.extract_urls_from_main_page_geo()

        self.bar_update(new_len=len(self.cards_urls),
                        new_text='Obtaining data...')
        self.get_data_from_urls()

        self.compile_results_df()

        self.delist_all_properties()  # all properties become delisted , unless they are still posted online
        self.compile_results_df_to_db()
        self.conn.close()

        self.bar_set("completed!  ")

    def get_df_caprates(self,map_id,threshold_date=None):
        """
        generate the dataframe for the caprates
        :param date_string: format %YYYY-%mm-%dd
        """
        if threshold_date is None:
            threshold_date = datetime.today().strftime('%Y-%m-%d')

        self.get_joined_data_as_dataframe(threshold_date,map_id)
        self.generate_df_caprates()

