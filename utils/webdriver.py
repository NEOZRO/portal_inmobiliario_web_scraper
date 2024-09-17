import ast
import pandas as pd

from bs4 import BeautifulSoup
import random
from time import sleep

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.proxy import Proxy, ProxyType
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By

import numpy as np
from utils.data_extractors import DataExtractor
from utils.progress_bar import ProgressBar

class WebDriver(DataExtractor,ProgressBar):
    """ webdriver class with all the functions """

    def __init__(self):
        super().__init__()

        self.total_number_of_properties_pages = None
        self.operation = None
        self.list_tipos_inmueble = None
        self.n_properties_dict = None
        self.list_operations = None
        self.dict_len_type_operations={"arriendo":0, "venta":0}
        self.current_type_operation = []
        self.ip_status_index = 1  # Set to zero to start with our ip at first
        self.previus_ip_index = -1
        self.download_chrome_webdriver()
        self.current_url = None
        self.picked_pts_features = None
        self.type = None
        self.tipo_operacion = None

        self.total_number_request = None
        self.uf = None


    def proxy_generate_bright_data(self):

        # username = 'brd-customer-hl_fb6fe685-zone-residential_proxy1'
        # password = 'wywp20qz9f5u'
        # port = 22225
        # session_id  = random.random()
        # super_proxy_url = ('http://%s-session-%s:%s@brd.superproxy.io:%d' %
        #                    (username, session_id, password, port))

        username = 'brd-customer-hl_aaf60d4d-zone-residential_proxy_x'
        password = '0r3sudcgsaud'
        port = 22225
        session_id = random.random()
        super_proxy_url = ('http://%s-session-%s:%s@brd.superproxy.io:%d' %
                           (username, session_id, password, port))

        print("proxy enabled!, random session id: ", session_id)
        return super_proxy_url
    @staticmethod
    def download_chrome_webdriver():
        """ simple execution to download chrome webdriver in the initialization """
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        webdriver.Chrome(options=chrome_options)

    def init_webdriver(self, get_images=False):
        """ initialize chrome webdriver"""

        ua = UserAgent()
        chrome_options = Options()

        chrome_options.add_argument("--headless=new") # Ensure GUI is off - works with proxies
        chrome_options.add_argument("--incognito")

        chrome_options.add_argument(f'user-agent={ua.random}')  # random user agent

        # proxy
        if self.ip_status_index > 0 and self.ip_status_index > self.previus_ip_index:
            proxy = self.proxy_generate_bright_data()
            chrome_options.proxy = Proxy({ 'proxyType': ProxyType.MANUAL, 'httpProxy' : proxy,"sslProxy":proxy})
            self.previus_ip_index=self.ip_status_index


        # skip images (it affects some part of the whole process)
        if not get_images:
            chrome_options.add_argument('--blink-settings=imagesEnabled=false')  # command to avoid images

        chrome_options.set_capability("acceptInsecureCerts", True)

        self.driver = webdriver.Chrome(options=chrome_options)

        self.driver.set_page_load_timeout(30)

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
            self.bar_set("Extracting url's from main pages. Restarting webdriver...")

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

    def get_total_number_of_properties_in_location(self, min_lat, max_lat, min_lon, max_lon, operacion, tipo):
        """
        get total number of properties in location
        :param lat-log: latitude and longitud of the location/area
        :return: number of properties in location
        """
        self.geo_url_main = f"https://www.portalinmobiliario.com/{operacion}/{tipo}/_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}"

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
        else:
            n_real_state = 0

        self.total_number_of_properties_pages += np.ceil(n_real_state/ 50).astype(int)
        self.n_properties_dict[tipo][operacion] = n_real_state

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

        tipos_inmueble = ["casa","departamento"]
        tipos_de_operacion = ["arriendo", "venta"]
        self.n_properties_dict = {}
        self.total_number_of_properties_pages = 0
        self.cards_urls = []

        for tipo in tipos_inmueble:
            self.type = tipo
            self.n_properties_dict[tipo] = {}
            for operacion in tipos_de_operacion:
                self.get_total_number_of_properties_in_location(min_lat,
                                                                max_lat,
                                                                min_lon,
                                                                max_lon,
                                                                operacion,
                                                                tipo)


        total = self.total_number_of_properties_pages
        self.bar_update(total,"Extracting url's from main pages")

        for tipo in tipos_inmueble:
            for operacion in tipos_de_operacion:
                self.type = tipo
                self.tipo_operacion = operacion
                n_main_pages = np.ceil(self.n_properties_dict[tipo][operacion]/ 50).astype(int)
                for i in range(1, n_main_pages + 1):
                    url_extra_string = "_Desde_" + str(int(np.floor((i - 1) / 2) * 100 + 1)) if i >= 3 else ""
                    geo_url = f"https://www.portalinmobiliario.com/{operacion}/{tipo}/{url_extra_string}_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}#{i}"

                    self.current_url = geo_url
                    self.main_soup = self.webdriver_request(geo_url,
                                                            wait=4,
                                                            xpath_wait="//div[@class='ui-search-map-list ui-search-map-list__item']")  # pair n_page takes time to update links
                    self.get_urls_from_containers()
                    self.bar.update(1)


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
        search_text = '"Características del inmueble",' # main identifier of the table in lazyload state
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

    def get_correct_soup_from_url(self, url):
        """
        Portalinmobiliario has 2 diferents pages, that randomly change.
         This function verify if url is correct to extract more data
        :return: return the soup of correct page
        """
        try: # to catch timeout exceptions

            soup = self.webdriver_request(url)

        except:
            self.ip_status_index += 1
            self.close_webdriver()
            self.init_webdriver(get_images=False)
            pass

        # in case page did load properly
        # if table data not avaliable or location data not avaliable --> refresh until it is for both

        iter_count=0
        while self.check_if_location_not_avaliable(soup) or not self.check_if_table_properties_avaliable(soup): # noqa
            iter_count+=1
            soup = self.webdriver_refresh(wait=4)

            if iter_count > 3:
                self.close_webdriver()
                self.init_webdriver(get_images=False)
                print("Restarting webdriver, from get correct soup")
                iter_count=0

        sleep(random.uniform(0, 3))

        return soup

    def check_if_table_properties_avaliable(self, soup):

        """ check if the soup has the data atributes table avaliable, returns if succeful or not"""

        try:
            self.table_data = self.get_data_real_state_table(soup)
            return True

        except Exception as e:
            return False



