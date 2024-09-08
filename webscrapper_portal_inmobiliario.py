
from tqdm.auto import tqdm
from time import sleep, time
import numpy as np
from logs import log_exception_str
from database import *

import ssl
from utils import WebDriver,ExchangeVariables,DataExtractor,InteractiveMap,ProgressBar

class WebScraperPortalInmobiliario(WebDriver,ExchangeVariables,DataExtractor,InteractiveMap,ProgressBar):
    """
    Created by Brian Lavin
    https://www.linkedin.com/in/brian-lavin/
    ---
    webscrappoing data from portal inmobiliario
    Works drawing a POLYGON into the map
    https://www.portalinmobiliario.com/
    :param tipo_operacion: select one -> ["arriendo","venta"]
    :param tipo_inmueble: select one -> ["casa","departamento"]
    :param theme: select one -> ["dark","default","white"]
    :return: df_results, xlsx file (optional)
    """

    def __init__(self, tipo_operacion, tipo_inmueble, theme="default", folder_save_name: str = None, debug=False):
        super().__init__()

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
        self.picked_pts_features = []
        self.metraje_total = []
        self.metraje_util = []
        self.ubicacion = []
        self.title = []
        self.price_symbol = []
        self.precios = []
        self.n_banos = []
        self.n_dormitorios = []
        self.metraje = []
        self.cards_containers = []
        self.cards_urls = []
        self.error_msg = []  #   DEBUG
        self.error_soups = []  #  DEBUG
        self.error_links = []  #  DEBUG

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

        # keep this line only if you trust your proxy provider
        ssl._create_default_https_context = ssl._create_unverified_context # NOQA.

        self.database_name = "real_state.db"
        self.current_time_str = datetime.now().strftime('%H%M_%d_%m_%y')
        self.theme = theme

        self.center_map_coordinates = (-33.4489, -70.6693)  # Stgo,Chile

        self.tipo_operacion = None
        self.type = tipo_inmueble

        if not isinstance(folder_save_name, str):
            raise ValueError("folder_save_name must be a string")

        self.folder_save_name = folder_save_name
        self.create_results_folder()

        self.get_uf_today()
        self.get_today_USD_CLP_value()
        self.init_webdriver(get_images=True)
        self.init_progress_bar(max_len = 100)
        self.total_number_request = 0

        if not debug:
            self.conn_db = create_conect_db(self.database_name)  # it creates only if it doesn't exist
            self.check_if_existing_property_project()

    def check_if_existing_property_project(self):
        """ if is already a folder with a poly selection. SKIP poly selection and just download the new data of the location """

        self.json_polygon_path = os.path.join(self.full_path, f'{self.folder_save_name}.json')  # NOQA
        if not os.path.exists(self.json_polygon_path):
            self.map_picker()
        else:
            try:
                self.load_json_polygon_selection()
                self.bar_set(f"UPDATING data for location found in {self.folder_save_name}")
                self.execute_main_process()

            except Exception as e:
                self.bar_set("Error, please check log file   ")
                insert_error_log(self.conn_db,
                                 self.folder_save_name,
                                 datetime.now(),
                                 self.current_url,
                                 log_exception_str(exception=e),
                                 False)

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

            self.tipo_operacion = self.list_operations[i]
            title, latitud, longitud, price_value, n_days_since_published = self.init_main_properties()
            self.init_dict_properties()

            soup = None
            try:
                self.bar.update(1)
                self.current_url = url

                init_time = time()

                self.bar_set(f'{self.tipo_operacion}: Obtaining data...[get right soup]')
                soup = self.get_correct_soup_from_url(url)

                self.bar_set(f'{self.tipo_operacion}: Obtaining data...[data from table]')
                self.get_data_from_table_in_url()

                self.bar_set(f'{self.tipo_operacion}: Obtaining data...[extracting features]')
                title, latitud, longitud, price_value, n_days_since_published = self.get_main_properties_from_soup(soup)

                self.bar_set(f'{self.tipo_operacion}: Obtaining data...[saving results]')
                self.append_all_propierties(title,latitud,longitud,price_value,n_days_since_published)

                end_time = time()

                if self.total_number_request >= 100 or end_time - init_time > 20:
                    self.bar_set(f'{self.tipo_operacion}: Obtaining data...[Changing proxy]')
                    self.ip_status_index += 1
                    self.close_webdriver()
                    sleep(5)
                    self.init_webdriver(get_images=False)

            except Exception as e:
                insert_error_log(self.conn_db,
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
                    self.bar_set(f'{self.tipo_operacion}: Obtaining data...[Changing proxy]')
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
            "tipo_inmueble": self.tipo_inmueble,
            "orientacion": self.orientacion,
            "gastos_comunes": self.GC,
            "titulo": self.title,
            "link": self.cards_urls}

        self.df_results = pd.DataFrame(dict_df)

    def compile_results_df_to_db(self):
        """ adds the results from df to the database"""

        for i, row in self.df_results.iterrows():
            insert_or_update_property(self.conn_db,
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
                                             self.folder_save_name,
                                             True))
            insert_price_history(self.conn_db,
                                 (row.latitud,row.longitud,row.titulo),
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

        # ARRIENDO
        self.tipo_operacion = "arriendo"
        self.extract_urls_from_main_page_geo()

        self.bar_update(new_len=len(self.cards_urls),
                        new_text='Obtaining data...')
        self.get_data_from_urls()

        self.compile_results_df()

        delist_all_properties(self.conn_db)  # all properties become delisted , unless they are still posted online
        self.compile_results_df_to_db()
        self.conn_db.close()

        self.bar_set("completed!  ")

    def handle_draw(self, self_2, action, geo_json):
        """internal thread of map picker to run funtions once the selection is completed"""
        try:
            # save of picked point
            self.picked_pts_features.append(geo_json)
            # self.save_json_polygon_selection()
            # self.execute_main_process()
            # self.save_visualization_map_polygon_selection()

        except Exception as e:
            self.bar_set("Error, please check log file   ")

