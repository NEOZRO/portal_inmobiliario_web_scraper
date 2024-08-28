import pandas as pd
from bs4 import BeautifulSoup
import requests
from tqdm.auto import tqdm
from time import sleep, time
import numpy as np

from ipyleaflet import Map, DrawControl, TileLayer, GeoJSON
import re
from IPython.display import display
import json
import random

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from fake_useragent import UserAgent

from logs import log_exception_str
from database import *

import ast
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class WebScraperPortalInmobiliario:
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
        self.bar_progress_get_data = None
        self.df_inversion_venta = None
        self.df_inversion_arriendo = None
        self.full_path = None
        self.current_url = None
        self.last_soup_debug = None

        self.database_name = "real_state.db"
        self.current_time_str = datetime.now().strftime('%H%M_%d_%m_%y')
        self.theme = theme
        self.webdriver_path = '.\chromedriver.exe'

        self.ip_blocked_status_index = 0
        # self.proxy_list = FreeProxy().get_proxy_list()

        self.sleep_time = 4  # lower sleep times between requests may produce temporal bans

        self.center_map_coordinates = (-33.4489, -70.6693)  # Stgo,Chile

        self.tipo_operacion = tipo_operacion
        self.type = tipo_inmueble

        if not isinstance(folder_save_name, str):
            raise ValueError("folder_save_name must be a string")

        self.folder_save_name = folder_save_name
        self.create_results_folder()

        self.get_uf_today()
        self.get_today_USD_CLP_value()
        self.init_webdriver(get_images=True)
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
                self.bar_progress_get_data = tqdm(total=100,
                                                  desc=f"UPDATING data for location found in {self.folder_save_name}")
                self.execute_main_process()

            except Exception as e:
                self.bar_progress_get_data.set_description("Error, please check log file   ")
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

    def get_uf_today(self):
        """
        return unit of UF of today to make convertion of prices later
        Sometimes at night the website is  updated with the UF value  of zero, affecting the price convertions
        """
        url_uf = 'https://www.uf-hoy.com/'
        div_id = 'valor_uf'
        response = requests.get(url_uf)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            div = soup.find('div', {'id': div_id})

            integer = int(div.get_text().split(",")[0].replace(".", ""))
            decimals = int(div.get_text().split(",")[1])

            value = integer + decimals / 100
            self.uf = value

    def get_today_USD_CLP_value(self):
        """ get conversion of USD/CLP from https://www.x-rates.com/ """

        page = requests.get('https://www.x-rates.com/calculator/?from=USD&to=CLP&amount=1')
        soup = BeautifulSoup(page.text, 'html.parser')

        result_text = soup.find(class_="ccOutputRslt").get_text(strip=True)
        value = np.float32(result_text.split("CLP")[0])
        self.usd_clp = value

    def proxy_generate_bright_data(self):
        username = 'brd-customer-hl_fb6fe685-zone-residential_proxy1'
        password = 'wywp20qz9f5u'
        port = 22225
        session_id = random.random()
        super_proxy_url = ('http://%s-country-cl-city-algarrobo-route_err-pass_dyn-session-%s:%s@brd.superproxy.io:%d' %
                           (username, session_id, password, port))
        proxy_dict = {
            'http': super_proxy_url,
            'https': super_proxy_url}
        return proxy_dict["http"]


    def init_webdriver(self, get_images=False):
        """ initialize chrome webdriver"""
        # TODO PARA MEJORAR SE REQUERIRAN HEADER DINAMICOS JUNTO A PROXIES VARIABLES, SERVICIOS COMO ZEROROWS OFRECEN AMBAS FUNCIONALIDADES

        ua = UserAgent()
        chrome_options = Options()

        chrome_options.add_argument("--headless")  # Ensure GUI is off
        chrome_options.add_argument("--incognito")

        chrome_options.add_argument(f'user-agent={ua.random}')  # random user agent

        # proxy
        # if self.ip_blocked_status_index < 0:  # TODO SI TIENE EL IGUAL UTILIZARA LA IP NUESTRA EN PRIMERA INSTANCIA
            # self.proxy_list = FreeProxy().get_proxy_list()
        # proxy = random.choice(self.proxy_list)
        proxy = self.proxy_generate_bright_data()
        chrome_options.add_argument(f"--proxy-server={proxy}")

        # skip images (it affects some part of the whole process)
        if not get_images:
            chrome_options.add_argument('--blink-settings=imagesEnabled=false')  # command to avoid images

        service = Service(executable_path=self.webdriver_path)
        self.driver = webdriver.Chrome(options=chrome_options, service=service)

    def webdriver_request(self, url, wait=0, xpath_wait=None):
        """  request using chrome webdriver"""

        self.total_number_request += 1
        self.driver.get(url)

        random_percentage = random.uniform(0, 0.8)
        scroll_position = self.driver.execute_script(f"return document.body.scrollHeight * {random_percentage};")
        self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")

        if wait > 0:
            sleep(wait)  # WAIT UNTIL PAGE LOAD

        try:
            if xpath_wait is not None:
                random_percentage = random.uniform(0, 0.8)
                scroll_position = self.driver.execute_script(f"return document.body.scrollHeight * {random_percentage};")
                self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, xpath_wait))
                )

        except:
            self.close_webdriver()
            self.init_webdriver(get_images=True)
            self.webdriver_request(url, wait, xpath_wait)
            print("restarting webdriver")

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        return soup

    def webdriver_refresh(self, wait=0):
        """ refresh webdriver page"""
        self.driver.refresh()

        random_percentage = random.uniform(0, 0.8)
        scroll_position = self.driver.execute_script(f"return document.body.scrollHeight * {random_percentage};")
        self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")

        if wait > 0:
            sleep(wait)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        return soup

    def close_webdriver(self):
        """ close webdriver session"""
        self.driver.quit()
        self.total_number_request = 0

    def extract_urls_from_main_page_basic(self):
        """
        Extracts single data of houses from all cards in main pages
        :return: list with single house url
        """
        for i in range(1, self.n_pages * 50, 50):
            if self.tipo_operacion == 'arriendo':
                main_url = 'https://www.portalinmobiliario.com/' + self.tipo_operacion.lower().replace(" ",
                                                                                                       "-") + '/' + self.type + '/' + self.region + '/_Desde_' + str(
                    i)
            elif self.tipo_operacion == 'venta':
                # solo interesan propiedades usadas
                main_url = 'https://www.portalinmobiliario.com/' + self.tipo_operacion.lower().replace(" ",
                                                                                                       "-") + '/' + self.type + '/propiedades-usadas/' + self.region + '/_Desde_' + str(
                    i)
            else:
                raise ValueError('tipo de operacion no valido')

            self.main_soup = self.webdriver_request(main_url)
            self.get_layout_cards_containers()

        self.get_urls_from_containers()
        self.get_data_from_containers()

    def get_total_number_of_properties_in_location(self, min_lat, max_lat, min_lon, max_lon):
        """
        get total number of properties in location
        :param lat-log: latitude and longitud of the location/area
        :return: number of properties in location
        """
        self.geo_url_main = f"https://www.portalinmobiliario.com/{self.tipo_operacion}/{self.type}/_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}"

        soup = self.webdriver_request(self.geo_url_main,wait=3)
        search_text = 'melidata("add", "event_data", {"vertical":"REAL_ESTATE","query":"","limit":100,"offset":0,"total":'

        text = str(soup)
        search_text_index  = text.find(search_text)

        while search_text_index == -1:
            soup = self.webdriver_refresh(2)
            text = str(soup)
            search_text_index  = text.find(search_text)

        text_number = self.find_next_string(text,
                                            search_text).split(",")

        if text_number:
            n_real_state = int(text_number[0])
            return n_real_state
        else:
            return 0

    def check_if_page_contain_urls(self,soup):
        """check if the loaded page contain info to extract urls cards"""
        return len(soup.find_all('div', {'class': 'ui-search-map-list ui-search-map-list__item'}))>0

        # tbl = self.driver.find_element(By.XPATH, "//table[@class='andes-table']")


    def extract_urls_from_main_page_geo(self):
        """
        Extracts single data of houses from all cards in main page of geoselection
        :return: list with single house url
        """
        polygon_coordinates = np.asarray(self.picked_pts_features[0]["geometry"]["coordinates"][0])
        max_lon = np.max(polygon_coordinates[:, 0])
        min_lon = np.min(polygon_coordinates[:, 0])
        max_lat = np.max(polygon_coordinates[:, 1])
        min_lat = np.min(polygon_coordinates[:, 1])

        self.total_number_of_properties = self.get_total_number_of_properties_in_location(min_lat, max_lat, min_lon,
                                                                                          max_lon)
        print(f"*** total number of properties: {self.total_number_of_properties}")
        # in case no properties found. Exit
        if self.total_number_of_properties == 0:
            return

        n_main_pages = int(np.ceil(self.total_number_of_properties / 50))

        for i in range(1, n_main_pages + 1):
            url_extra_string = "_Desde_" + str(int(np.floor((i - 1) / 2) * 100 + 1)) if i >= 3 else ""
            geo_url = f"https://www.portalinmobiliario.com/{self.tipo_operacion}/{self.type}/{url_extra_string}_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}#{i}"

            print("------------------------")
            print(f"*** url: {geo_url}")
            self.current_url = geo_url
            self.main_soup = self.webdriver_request(geo_url,
                                                    wait=4,
                                                    xpath_wait="//div[@class='ui-search-map-list ui-search-map-list__item']")  # pair n_page takes time to update links

            self.get_layout_cards_containers(mode="geo")

        self.get_urls_from_containers(mode="geo")

    def get_layout_cards_containers(self, mode="basic"):
        """
        get all cards containers inside main page of search
        """
        if mode == "basic":
            self.cards_containers.extend(self.main_soup.find_all('li', {'class': 'ui-search-layout__item'}))
        elif mode == "geo":
            self.cards_containers.extend(
                self.main_soup.find_all('div', {'class': 'ui-search-map-list ui-search-map-list__item'}))

    def get_urls_from_containers(self, mode="basic"):
        """
        get all cards urls from each containers/cards list
        """
        if mode == "basic":
            for container in self.cards_containers:
                self.cards_urls.append(container.find('a', class_='ui-search-result__image ui-search-link')['href'])
        elif mode == "geo":
            for container in self.cards_containers:
                self.cards_urls.append(
                    container.find('a', class_='ui-search-result__main-image-link ui-search-link')['href'])

    def get_data_from_containers(self):
        """
        get all data avaliable inside each containers/cards list
        """
        for i, container in enumerate(self.cards_containers):
            list_attributes = container.find_all('li', {'class': 'ui-search-card-attributes__attribute'})
            found_attributes = [text.split(" ")[-1] for text in [atrib.text for atrib in list_attributes]]

            index_offset = 0
            # check if the atributes exists
            if 'dormitorios' in found_attributes or 'dormitorio' in found_attributes:
                self.n_dormitorios.append(int(list_attributes[0].text.split(" ")[0].replace('.', '')))
            else:
                index_offset -= 1
                self.n_dormitorios.append(np.NAN)
            if 'baños' in found_attributes or 'baño' in found_attributes:
                self.n_banos.append(int(list_attributes[1 + index_offset].text.split(" ")[0].replace('.', '')))
            else:
                index_offset -= 1
                self.n_banos.append(np.NAN)
            if "útiles" in found_attributes:
                self.metraje_util.append(int(list_attributes[2 + index_offset].text.split(" ")[0].replace('.', '')))
            else:
                self.metraje_util.append(np.NAN)

    def get_data_from_containers_geo(self):
        """
        get all data avaliable inside each containers/cards list
        """
        for i, container in enumerate(self.cards_containers):

            current_atribute = container.find_all('div', {'class': 'ui-search-result__content'})[0]
            found_attributes = current_atribute.find_all('div', {'class': 'ui-search-result__content-attributes'})[
                0].get_text().replace(u'\xa0', u' ').split(" ")

            if 'dormitorios' in found_attributes or 'dormitorio' in found_attributes:
                self.n_dormitorios.append(int(found_attributes[-2].replace(".", "")))
            else:
                self.n_dormitorios.append(np.NAN)

            if "útiles" in found_attributes:
                self.metraje_util.append(int(found_attributes[0].replace(".", "")))
            else:
                self.metraje_util.append(np.NAN)

            self.n_banos.append(np.NAN)

    def get_price_from_soup(self, soup):
        """
        get price from soup/url of a specific real state property page
        """

        price_symbol = soup.find_all("span", "andes-money-amount__currency-symbol")[0].text
        if price_symbol == "$":
            return int(soup.find_all("span", "andes-money-amount__fraction")[0].text.replace(".", ""))

        elif price_symbol == "UF":

            integer = int(soup.find_all("span", "andes-money-amount__fraction")[0].text.replace('.', ''))

            decimals_in_price = soup.find_all("span", "andes-money-amount__cents")
            if decimals_in_price:
                decimals = int(decimals_in_price[0].text)
            else:
                decimals = 0

            return (integer + decimals / 100) * self.uf

        elif price_symbol == "U$S":
            dolar_value = int(soup.find_all("span", "andes-money-amount__fraction")[0].text.split(".")[0].replace(",", ""))
            return np.round(dolar_value * self.usd_clp,2)

    def get_data_real_state_table(self, soup):
        """
        retrieve all the data from a table in the page
        try lazyload type of page then use xpath
        :param soup: soup of the page
        :return: df with the information
        """
        open_bracket_index = 0
        close_bracket_index = -1
        main_text = str(soup)
        search_text = "Características del inmueble" # main identifier of the table in lazyload state
        index = main_text.find(search_text)
        next_index = index + len(search_text)

        moving_index = 0
        while True:

            if main_text[next_index+moving_index] == "[" :
                open_bracket_index = next_index+moving_index+1

            if main_text[next_index+moving_index] == "]":
                close_bracket_index = next_index+moving_index
                break

            moving_index += 1
            if moving_index > 900:

                tbl = self.driver.find_element(By.XPATH, "//table[@class='andes-table']")
                table_data = pd.read_html(tbl.get_attribute('outerHTML'))[0]

                if len(table_data) > 0:
                    return table_data
                else:
                    pass
                break

        data_dict = main_text[open_bracket_index:close_bracket_index]

        data = ast.literal_eval(data_dict)

        final_data=[]
        for value in data:
            final_data.append([value["id"] ,
                               value["text"]])

        return pd.DataFrame(final_data)

    def get_data_from_table_in_url(self,soup):
        """
        looks for table inside urls and extract the needed data
        """
        # extracting dinamic table data into dict
        for i, row in self.table_data.iterrows():

            if row.loc[0] == "Tipo de casa" or row.loc[0] == "Tipo de departamento":
                self.total_dict_properties["Tipo de inmueble"] = row.loc[1]

            elif row.loc[0] in ["Superficie total", "Superficie útil"]:
                self.total_dict_properties[row.loc[0]] = float(row.loc[1].split(" ")[0])

            elif row.loc[0] in ["Orientación"]:
                self.total_dict_properties[row.loc[0]] = str(row.loc[1])

            elif row.loc[0] in ["Dormitorios", "Baños", "Estacionamientos", "Bodegas", "Cantidad de pisos",
                                "Número de piso de la unidad", "Antigüedad", "Gastos comunes"]:
                self.total_dict_properties[row.loc[0]] = int(row.loc[1].split(" ")[0].replace(".", ""))


    @staticmethod
    def find_next_string(text, search_text):
        """
        find next ocurrence/word in a string sequence based on a search word
        :param text: text in wich to find
        :param search_text: text that will be searched
        :return: next word in the sequence
        """

        index = text.find(search_text)

        if index != -1:
            next_index = index + len(search_text)
            if next_index < len(text):
                next_string = text[next_index:].split()[0]
                return next_string
            else:
                return None
        else:
            return None


    def get_days_since_published(self, soup):
        """
        get the days since the publcation started
        :param soup: right soup with all the data
        :return: number of days, int
        """
        multiplier = 1


        list_grabbers_publications_days = [
            "ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-header__bottom-subtitle",
            "ui-pdp-background-color--WHITE ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-header__bottom-subtitle",
            "ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-seller-validated__title",
            "ui-pdp-subtitle ui-pdp-subtitle_rex"]
        list_types_publications_days = [
            "p",
            "p",
            "p",
            "span"]
        index_grabber = 0

        while True: # make break condition and a break
            if index_grabber < len(list_grabbers_publications_days):
                days_publication_line = soup.find_all(list_types_publications_days[index_grabber],
                                                      list_grabbers_publications_days[index_grabber])


                if days_publication_line:

                    if  days_publication_line[0].text.find("hoy") != -1:
                        period = "dia"
                        quantity = 1
                    elif days_publication_line[0].text.find("semana") != -1:
                        period = "dia"
                        quantity = 7

                    else:
                        quantity = int(WebScraperPortalInmobiliario.find_next_string(days_publication_line[0].text, "hace"))
                        period = WebScraperPortalInmobiliario.find_next_string(days_publication_line[0].text, str(quantity))
                    break
                else:
                    index_grabber += 1
            else:
                # could not get days since published
                return 0


        if period == "meses" or period == "mes":
            multiplier = 30

        elif period == "años" or period == "año":
            multiplier = 365

        final_days = multiplier * quantity

        return final_days

    def get_latitud_longitud_from_soup(self, soup):
        """
        get latitude and longitude from soup
        :return: latitude and longitude, list
        """
        url_text_w_coordenates = \
        soup.find_all('div', {"id": "ui-vip-location__map"})[0].find('img').get('srcset').split("=")[5]

        latitud = url_text_w_coordenates.split("%")[0]
        pattern_longitud = r"%2C(.*?)&"  # Pattern to match text between '%2C' and '&'
        longitud = re.findall(pattern_longitud, url_text_w_coordenates)[0]

        return float(latitud), float(longitud)

    def get_data_from_urls(self):
        """
        iterate over all the urls found in the location for each house/real state
        Saves the data into lists to later make a dataframe
        """
        self.close_webdriver()
        self.init_webdriver(get_images=False)

        for url in self.cards_urls:

            latitud = np.NAN
            longitud = np.NAN
            price_value = np.NAN
            n_days_since_published = np.NAN
            title = np.NAN
            ubicacion_string = np.NAN
            self.total_dict_properties = {'Superficie total': np.nan,
                                         'Superficie útil': np.nan,
                                         'Dormitorios': np.nan,
                                         'Baños': np.nan,
                                         'Bodegas': np.nan,
                                         'Cantidad de pisos': np.nan,
                                         'Tipo de inmueble': np.nan,
                                         'Orientación': np.nan,
                                         'Antigüedad': np.nan,
                                         'Gastos comunes': np.nan,
                                         "Número de piso de la unidad": np.nan,
                                         "Estacionamientos": np.nan
                                         }
            soup = None
            try:
                self.bar_progress_get_data.update(1)
                self.current_url = url

                init_time = time()
                soup = self.get_correct_soup_from_url(url)
                self.get_data_from_table_in_url(soup)

                self.last_soup_debug = soup

                title = soup.find_all("h1", "ui-pdp-title")[0].text
                price_value = self.get_price_from_soup(soup)

                # ubicacion_string = [h2 for h2 in soup.find_all('h2') if h2.text == "Ubicación"][0].parent.text.split(
                #     "Ver información")[0].split("Ubicación")[1]

                n_days_since_published = self.get_days_since_published(soup) # TODO ERROR. EVERITHING IS 1
                latitud, longitud = self.get_latitud_longitud_from_soup(soup)


                # save in global list
                self.title.append(title)
                self.latitud.append(latitud)
                self.longitud.append(longitud)
                self.precios.append(price_value)
                self.dias_desde_publicacion.append(n_days_since_published)
                self.superficie_total.append(self.total_dict_properties["Superficie total"])
                self.superficie_util.append(self.total_dict_properties["Superficie útil"])
                self.n_dormitorios.append(self.total_dict_properties["Dormitorios"])
                self.n_banos.append(self.total_dict_properties["Baños"])
                self.estacionamientos.append(self.total_dict_properties["Estacionamientos"])
                self.bodegas.append(self.total_dict_properties["Bodegas"])
                self.cantidad_pisos.append(self.total_dict_properties["Cantidad de pisos"])
                self.piso_unidad.append(self.total_dict_properties["Número de piso de la unidad"])
                self.tipo_inmueble.append(self.total_dict_properties["Tipo de inmueble"])
                self.orientacion.append(self.total_dict_properties["Orientación"])
                self.antiguedad.append(self.total_dict_properties["Antigüedad"])
                self.GC.append(self.total_dict_properties["Gastos comunes"])
                # self.ubicacion.append(ubicacion_string)

                end_time = time()
                # si se hacen mas de X request o se demora mas de 20 segundos en obtener  la data... esta siendo blioqueada la IP
                if self.total_number_request >= 200 or end_time - init_time > 20:
                    self.ip_blocked_status_index += 1
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
                self.title.append(title)
                self.latitud.append(latitud)
                self.longitud.append(longitud)
                self.precios.append(price_value)
                self.dias_desde_publicacion.append(n_days_since_published)
                self.superficie_total.append(self.total_dict_properties["Superficie total"])
                self.superficie_util.append(self.total_dict_properties["Superficie útil"])
                self.n_dormitorios.append(self.total_dict_properties["Dormitorios"])
                self.n_banos.append(self.total_dict_properties["Baños"])
                self.estacionamientos.append(self.total_dict_properties["Estacionamientos"])
                self.bodegas.append(self.total_dict_properties["Bodegas"])
                self.cantidad_pisos.append(self.total_dict_properties["Cantidad de pisos"])
                self.piso_unidad.append(self.total_dict_properties["Número de piso de la unidad"])
                self.tipo_inmueble.append(self.total_dict_properties["Tipo de inmueble"])
                self.orientacion.append(self.total_dict_properties["Orientación"])
                self.antiguedad.append(self.total_dict_properties["Antigüedad"])
                self.GC.append(self.total_dict_properties["Gastos comunes"])
                self.ubicacion.append(ubicacion_string)

                continue
    def check_if_location_not_avaliable(self, soup):
        """ check if location loaded correctly """
        return len(soup.find_all('div', {"id": "ui-vip-location__map"})) == 0

    def check_if_table_properties_avaliable(self, soup):

        """ check if the soup has the data atributes table avaliable, returns if succeful or not"""

        try:
            self.table_data = self.get_data_real_state_table(soup)
            return True

        except Exception as e:
            return False

    def get_correct_soup_from_url(self, url):
        """
        Portalinmobiliario has 2 diferents pages, that randomly change.
         This function verify if url is correct to extract more data
        :return: return the soup of correct page
        """
        soup = self.webdriver_request(url)

        # in case page did load properly
        # if table data not avaliable or location data not avaliable --> refresh until it is for both

        iter_count=0
        while self.check_if_location_not_avaliable(soup) or not self.check_if_table_properties_avaliable(soup):
            iter_count+=1
            soup = self.webdriver_refresh(wait=4)

            if iter_count > 5:
                self.close_webdriver()
                self.init_webdriver(get_images=False)
                print("Restarting webdriver, from get correct soup")
                iter_count=0

        sleep(random.uniform(0, 3))

        return soup

    def compile_results_df(self):
        """ generate the final dataframe of results"""
        dict_df = {
            "latitud": self.latitud,
            "longitud": self.longitud,
            "precio": self.precios,
            "precio_UF": np.asarray(self.precios) / self.uf,
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
            insert_or_update_property(self.conn_db, (row.latitud,
                                                     row.longitud,
                                                     row.dias_desde_publicacion,
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
                                                     row.titulo,
                                                     # row.ubicacion,
                                                     row.link,
                                                     self.folder_save_name,
                                                     True))
            insert_price_history(self.conn_db,
                                 row.latitud,
                                 row.longitud,
                                 row.titulo,
                                 row.precio,
                                 row.precio_UF,
                                 self.tipo_operacion,
                                 datetime.now())

    def save_results(self, filename):
        """ save data into a excel file, saved in the same folder of this script"""

        path_save = os.path.join(self.full_path, filename)
        self.df_results.to_excel(path_save)

    def create_results_folder(self):
        """ create folder for results """
        if not os.path.exists("results"):
            os.makedirs("results")

        self.full_path = os.path.join("results", self.folder_save_name)
        if not os.path.exists(self.full_path):
            os.makedirs(self.full_path)

    def save_visualization_map_polygon_selection(self):
        """ save map selection as html visualization """
        path_save = os.path.join(self.full_path, f'{self.folder_save_name}.html')
        path_load = self.json_polygon_path
        with open(path_load, 'r') as json_file:
            data = json.load(json_file)

        geo_json = GeoJSON(data=data)
        polygon_coordinates_array = np.asarray(data["geometry"]["coordinates"][0])

        map_to_save = self.init_map_ipyflet(theme=self.theme,
                                            center_map_cordinates=(np.mean(polygon_coordinates_array[:, 1]),
                                                                   np.mean(polygon_coordinates_array[:, 0])),
                                            zoom=14)

        map_to_save.add(geo_json)
        map_to_save.save(path_save)

    def save_json_polygon_selection(self):
        """ save map selection as json """
        path_save = os.path.join(self.full_path, f'{self.folder_save_name}.json')
        with open(path_save, 'w') as json_file:
            json.dump(self.picked_pts_features[0], json_file)

    def load_json_polygon_selection(self):
        """ load map selection as json """
        path_load = self.json_polygon_path
        with open(path_load, 'r') as json_file:
            self.picked_pts_features = [json.load(json_file)]

    def execute_main_process(self):
        """ main process"""
        if self.tipo_operacion == "venta" or self.tipo_operacion == "arriendo":

            self.extract_urls_from_main_page_geo()

            WebScraperPortalInmobiliario.update_info_progress_tqdm_bar(self.bar_progress_get_data,
                                                                       new_len=len(self.cards_urls),
                                                                       new_text=f'{self.tipo_operacion}: Obtaining data...')
            self.get_data_from_urls()
            sleep(3)

            self.compile_results_df()

            delist_all_properties(self.conn_db)  # all properties become delisted , unless they are still posted online
            self.compile_results_df_to_db()


        elif self.tipo_operacion == "inversion":

            # ARRIENDO
            self.tipo_operacion = "arriendo"
            self.extract_urls_from_main_page_geo()

            WebScraperPortalInmobiliario.update_info_progress_tqdm_bar(self.bar_progress_get_data,
                                                                       new_len=len(self.cards_urls),
                                                                       new_text='Arriendo: Obtaining data...')
            self.get_data_from_urls()
            sleep(3)

            self.compile_results_df()

            delist_all_properties(self.conn_db)  # all properties become delisted , unless they are still posted online
            self.compile_results_df_to_db()

            # VENTA
            self.tipo_operacion = "venta"
            self.empty_lists_except_specific("picked_pts_features")

            self.extract_urls_from_main_page_geo()
            sleep(3)

            WebScraperPortalInmobiliario.update_info_progress_tqdm_bar(self.bar_progress_get_data,
                                                                       new_len=len(self.cards_urls),
                                                                       new_text='Venta: Obtaining data...')
            self.get_data_from_urls()
            self.compile_results_df()
            self.compile_results_df_to_db()

            self.conn_db.close()
        else:

            self.bar_progress_get_data.set_description("Error")
            raise ValueError("Type must be 'inversion', 'venta' or 'arriendo'")

        self.bar_progress_get_data.set_description("completed!   ")

    def handle_draw(self, self_2, action, geo_json):
        """internal thread of map picker to run funtions once the selection is completed"""
        try:
            # save of picked point
            self.picked_pts_features.append(geo_json)
            self.save_json_polygon_selection()
            self.execute_main_process()
            self.save_visualization_map_polygon_selection()

        except Exception as e:
            self.bar_progress_get_data.set_description("Error, please check log file   ")

    @staticmethod
    def update_info_progress_tqdm_bar(bar, new_len, new_text):
        """
        update inside descriptor of tqdm bar
        :param bar: tqdm method
        :param new_len: new len
        :param new_text: new text
        """
        bar.total = new_len
        bar.reset(total=new_len)
        bar.set_description(new_text)
        bar.refresh()

    def init_map_ipyflet(self, center_map_cordinates, theme="default", zoom=13):
        """ init Map from ipyfleet based con configurations"""

        dark_map_layer = TileLayer(
            url="https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}.png	",
            attribution='&copy; <a href="https://carto.com/">Carto</a>',
            name='Neon'
        )

        white_map_layer = TileLayer(
            url="https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png",
            attribution='&copy; <a href="https://carto.com/">Carto</a>',
            name='white'
        )
        map = None

        if theme == "dark":

            map = Map(layers=[dark_map_layer, ], center=center_map_cordinates, zoom=zoom)

        elif theme == "default":

            map = Map(center=center_map_cordinates, zoom=zoom)

        elif theme == "white":
            map = Map(layers=[white_map_layer, ], center=center_map_cordinates, zoom=zoom)

        return map

    def map_picker(self):
        """
            allows the user to pick from global map a location to search in
            more themes: https://wiki.openstreetmap.org/wiki/Raster_tile_providers
            """
        self.main_interactive_map = self.init_map_ipyflet(theme=self.theme,
                                                          center_map_cordinates=self.center_map_coordinates)

        draw_control = DrawControl()

        draw_control.on_draw(self.handle_draw)

        self.main_interactive_map.add(draw_control)
        display(self.main_interactive_map)  # NOQA

        self.bar_progress_get_data = tqdm(total=100, desc="SELECT REGION TO ANALYZE ...")

    def save_soup_as_json(self, soup):
        """save soup as json"""
        with open("soup.json", "w", encoding="utf-8") as file:
            # BEAUTIFUL JSON BEFORE SAVE
            soup = soup.prettify()
            json.dump(str(soup), file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    # execute on jupyter only because of the dinamic map
    WebScraperPortalInmobiliario(tipo_operacion="venta",
                                 tipo_inmueble="casa",
                                 theme="default",
                                 folder_save_name="test"
                                 )
