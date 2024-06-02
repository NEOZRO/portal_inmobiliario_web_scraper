from bs4 import BeautifulSoup
import requests
from tqdm.auto import tqdm
from time import sleep
import numpy as np
import pandas as pd
from datetime import datetime
from ipyleaflet import Map, DrawControl, TileLayer
import re
from IPython.display import display


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
    :param save_data: Bolean -> excel created from results and  saved in this file path
    :param theme: select one -> ["dark","default","white"]
    :return: df_results, xlsx file (optional)

    """
    def __init__(self,tipo_operacion,tipo_inmueble, save_data=True, theme="default"):

        self.longitud = []
        self.latitud = []
        self.dias_desde_publicacion = []
        self.piso_unidad = []
        self.GC = []
        self.antiguedad = []
        self.orientacion = []
        self.tipo_casa = []
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

        self.main_soup = None
        self.uf = None
        self.region=None
        self.n_pages=None
        self.main_interactive_map = None
        self.geo_url_main = None
        self.total_number_of_properties = None
        self.df_results = None

        self.save_data = save_data
        self.theme = theme

        self.headers_request = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
        self.request_timeout=10
        self.sleep_time = 2 # lower sleep times between requests may produce temporal bans
        # paid web scraping API's should fix this

        self.center_map_coordinates = (-33.4489, -70.6693) # Stgo,Chile

        self.tipo_operacion = tipo_operacion
        self.type=tipo_inmueble


        self.get_uf_today()
        self.map_picker()



    def get_uf_today(self):
        """
        return unit of UF of today to make convertion of prices later
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
            self.uf =  value
    def extract_urls_from_main_page_basic(self):
        """
        Extracts single data of houses from all cards in main pages
        :return: list with single house url
        """
        for i in range(1,self.n_pages*50,50):
            if self.tipo_operacion == 'arriendo':
                main_url = 'https://www.portalinmobiliario.com/'+self.tipo_operacion.lower().replace(" ","-")+'/'+self.type+'/'+self.region+'/_Desde_'+ str(i)
            elif self.tipo_operacion == 'venta':
                # solo interesan propiedades usadas
                main_url = 'https://www.portalinmobiliario.com/'+self.tipo_operacion.lower().replace(" ","-")+'/'+self.type+'/propiedades-usadas/'+self.region+'/_Desde_'+ str(i)
            else:
                raise ValueError('tipo de operacion no valido')

            main_response = requests.get(main_url,headers=self.headers_request, timeout=self.request_timeout)
            self.main_soup = BeautifulSoup(main_response.text,'html.parser')
            self.get_layout_cards_containers()

        self.get_urls_from_containers()
        self.get_data_from_containers()

    def get_total_number_of_properties_in_location(self,min_lat,max_lat,min_lon,max_lon):
        """
        get total number of properties in location
        :param lat-log: latitude and longitud of the location/area
        :return: number of properties in location
        """
        self.geo_url_main = f"https://www.portalinmobiliario.com/{self.tipo_operacion}/{self.type}/_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}"
        main_response = requests.get(self.geo_url_main,headers=self.headers_request, timeout=self.request_timeout)
        soup = BeautifulSoup(main_response.text,'html.parser')
        find_header_text = soup.find_all('div',{'class':'ui-search-map-list ui-search-map-list__header'})
        if len(find_header_text) == 0:
            return 0
        else:
            return int(find_header_text[0].get_text().split("inmueble")[0].split(" ")[-2].replace(".",""))


    def extract_urls_from_main_page_geo(self):
        """
        Extracts single data of houses from all cards in main page of geoselection
        :return: list with single house url
        """
        polygon_coordinates = np.asarray(self.picked_pts_features[0]["geometry"]["coordinates"][0])
        max_lon = np.max(polygon_coordinates[:,0])
        min_lon = np.min(polygon_coordinates[:,0])
        max_lat = np.max(polygon_coordinates[:,1])
        min_lat = np.min(polygon_coordinates[:,1])

        self.total_number_of_properties = self.get_total_number_of_properties_in_location(min_lat,max_lat,min_lon,max_lon)

        # in case no properties found. Exit
        if self.total_number_of_properties == 0:
            return

        n_main_pages = int(np.ceil(self.total_number_of_properties/50))

        for i in range(1,n_main_pages+1):
            print(i)

            url_extra_string = "_Desde_"+str(int(np.floor(i/2) * 100+ 1)) if i>=3 else ""
            geo_url = f"https://www.portalinmobiliario.com/{self.tipo_operacion}/{self.type}/{url_extra_string}_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}#{i}"

            print(geo_url)

            main_response = requests.get(geo_url,headers=self.headers_request, timeout=self.request_timeout)
            sleep(self.sleep_time)
            self.main_soup = BeautifulSoup(main_response.text,'html.parser')
            self.get_layout_cards_containers(mode="geo")

        self.get_urls_from_containers(mode="geo")


    def get_layout_cards_containers(self, mode="basic"):
        """
        get all cards containers inside main page of search
        """
        if mode=="basic":
            self.cards_containers.extend(self.main_soup.find_all('li',{'class':'ui-search-layout__item'}))
        elif mode=="geo":
            self.cards_containers.extend(self.main_soup.find_all('div',{'class':'ui-search-map-list ui-search-map-list__item'}))

    def get_urls_from_containers(self, mode="basic"):
        """
        get all cards urls from each containers/cards list
        """
        if mode=="basic":
            for container in self.cards_containers:
                self.cards_urls.append(container.find('a',class_='ui-search-result__image ui-search-link')['href'])
        elif mode=="geo":
            for container in self.cards_containers:
                self.cards_urls.append(container.find('a',class_='ui-search-result__main-image-link ui-search-link')['href'])

    def get_data_from_containers(self):
        """
        get all data avaliable inside each containers/cards list
        """
        for i, container in enumerate(self.cards_containers):
            list_attributes = container.find_all('li',{'class':'ui-search-card-attributes__attribute'})
            found_attributes = [text.split(" ")[-1] for text in [atrib.text for atrib in list_attributes]]

            index_offset = 0
            # check if the atributes exists
            if 'dormitorios' in found_attributes or  'dormitorio' in found_attributes:
                self.n_dormitorios.append(int(list_attributes[0].text.split(" ")[0].replace('.', '')))
            else:
                index_offset-=1
                self.n_dormitorios.append(np.NAN)
            if 'baños' in found_attributes or  'baño' in found_attributes:
                self.n_banos.append(int(list_attributes[1+index_offset].text.split(" ")[0].replace('.', '')))
            else:
                index_offset-=1
                self.n_banos.append(np.NAN)
            if "útiles" in found_attributes:
                self.metraje_util.append(int(list_attributes[2+index_offset].text.split(" ")[0].replace('.', '')))
            else:
                self.metraje_util.append(np.NAN)

    def get_data_from_containers_geo(self):
        """
        get all data avaliable inside each containers/cards list
        """
        for i, container in enumerate(self.cards_containers):

            current_atribute = container.find_all('div',{'class':'ui-search-result__content'})[0]
            found_attributes = current_atribute.find_all('div',{'class':'ui-search-result__content-attributes'})[0].get_text().replace(u'\xa0', u' ').split(" ")

            if 'dormitorios' in found_attributes or  'dormitorio' in found_attributes:
                self.n_dormitorios.append(int(found_attributes[-2].replace(".","")))
            else:
                self.n_dormitorios.append(np.NAN)

            if "útiles" in found_attributes:
                self.metraje_util.append(int(found_attributes[0].replace(".","")))
            else:
                self.metraje_util.append(np.NAN)

            self.n_banos.append(np.NAN)


    def get_price_from_soup(self,soup):
        """
        get price from soup/url of a specific real state property page
        """

        price_symbol = soup.find_all("span","andes-money-amount__currency-symbol")[0].text
        if price_symbol == "$":
            return int(soup.find_all("span","andes-money-amount__fraction")[0].text.replace(".",""))

        elif price_symbol == "UF":

            integer = int(soup.find_all("span","andes-money-amount__fraction")[0].text.replace('.', ''))

            decimals_in_price = soup.find_all("span","andes-money-amount__cents")
            if decimals_in_price:
                decimals = int(decimals_in_price[0].text)
            else:
                decimals = 0

            return (integer + decimals / 100) * self.uf

    def get_data_from_table_in_url(self, url):
        """
        looks for table inside urls and extract the needed data
        :param url: url of real state
        """
        total_dict_proerties = {'Superficie total': np.nan,
        'Superficie útil': np.nan,
        'Dormitorios': np.nan,
        'Baños': np.nan,
        'Bodegas': np.nan,
        'Cantidad de pisos': np.nan,
        'Tipo de casa': np.nan,
        'Orientación': np.nan,
        'Antigüedad': np.nan,
        'Gastos comunes': np.nan,
        "Número de piso de la unidad": np.nan,
        "Estacionamientos": np.nan
                                }


        # loop to get the right page.. from the two that can show up in 'portalinmobiliario'
        # ONE OF THEM DOESNT HAVE TH INFO NEEDED
        table_data = None
        wrong_page = True
        while wrong_page:
            try:
                table_data = pd.read_html(url)[0]
                sleep(self.sleep_time)
                wrong_page = False
            except:
                continue

        # extracting dinamic table data into dict

        for i,row in table_data.iterrows():
            try:
                # if numeric data
                if row.loc[1].split(" ")[0].replace(".","").isnumeric():
                    total_dict_proerties[row.loc[0]] = int(row.loc[1].split(" ")[0].replace(".",""))
                # if string
                else:
                    total_dict_proerties[row.loc[0]] = row.loc[1]
            except:
                # in case of errors
                total_dict_proerties[row.loc[0]] = np.nan

        # saving into the global list to later form the dataframe
        self.superficie_total.append(total_dict_proerties["Superficie total"])
        self.superficie_util.append(total_dict_proerties["Superficie útil"])
        self.n_dormitorios.append(total_dict_proerties["Dormitorios"])
        self.n_banos.append(total_dict_proerties["Baños"])
        self.estacionamientos.append(total_dict_proerties["Estacionamientos"])
        self.bodegas.append(total_dict_proerties["Bodegas"])
        self.cantidad_pisos.append(total_dict_proerties["Cantidad de pisos"])
        self.piso_unidad.append(total_dict_proerties["Número de piso de la unidad"])
        self.tipo_casa.append(total_dict_proerties["Tipo de casa"])
        self.orientacion.append(total_dict_proerties["Orientación"])
        self.antiguedad.append(total_dict_proerties["Antigüedad"])
        self.GC.append(total_dict_proerties["Gastos comunes"])

    def get_days_since_published(self,soup):
        """
        get the days since the publcation started
        :param soup: right soup with all the data
        :return: number of days, int
        """
        multiplier = 1
        # normal page
        try:
            period = soup.find_all("p","ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-header__bottom-subtitle")[0].text.split(" ")[3]
            quantity = int(soup.find_all("p","ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-header__bottom-subtitle")[0].text.split(" ")[2])

        # page of verified publishers
        except:
            period =soup.find_all("p","ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-seller-validated__title")[0].text.split(" ")[3]
            quantity=int(soup.find_all("p","ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--REGULAR ui-pdp-seller-validated__title")[0].text.split(" ")[2])

        if period=="meses" or period=="mes":
            multiplier = 30

        elif period=="años" or period=="año":
            multiplier = 365

        final_days = multiplier * quantity

        return final_days

    def get_latitud_longitud_from_soup(self,soup):
        """
        get latitude and longitude from soup
        :return: latitude and longitude, list
        """
        url_text_w_coordenates = soup.find_all('img', {"class":"ui-pdp-image", "decoding":"async"})[-1]["srcset"].split("=")[5]

        latitud = url_text_w_coordenates.split("%")[0]
        pattern_longitud = r"%2C(.*?)&"  # Pattern to match text between '%2C' and '&'
        longitud = re.findall(pattern_longitud, url_text_w_coordenates)[0]

        return float(latitud), float(longitud)

    def get_data_from_urls(self):
        """
        iterate over all the urls found in the location for each house/real state
        Saves the data into lists to later make a dataframe
        """

        for url in self.cards_urls:
            self.bar_progress_get_data.update(1)

            soup = self.get_correct_soup_from_url(url)

            self.title.append(soup.find_all("h1","ui-pdp-title")[0].text)
            self.precios.append(self.get_price_from_soup(soup))
            self.ubicacion.append([h2 for h2 in soup.find_all('h2') if h2.text == "Ubicación"][0].parent.text.split("Ver información")[0].split("Ubicación")[1])
            self.dias_desde_publicacion.append(self.get_days_since_published(soup))
            lat,lon = self.get_latitud_longitud_from_soup(soup)
            self.latitud.append(lat)
            self.longitud.append(lon)
            self.get_data_from_table_in_url(url)


    def get_correct_soup_from_url(self,url):
        """
        Portalinmobiliario has 2 diferents pages, that randomly change.
         This function verify if url is correct to extract more data
        :return: return the soup of correct page
        """
        soup = None
        wrong_page = True
        while wrong_page:
            get_url = requests.get(url, headers=self.headers_request, timeout=self.request_timeout)
            soup = BeautifulSoup(get_url.text, "html.parser")
            sleep(self.sleep_time)
            all_h2 = [h2.text for h2 in soup.find_all('h2')]
            # if 'ubication' is in a secondary title (h2). Then its the good page. Continue
            if "Ubicación" in all_h2:
                wrong_page = False

        return soup

    def save_results(self):
        """ save data into a excel file, saved in the same folder of this script"""

        dict_df = {
            "titulo": self.title,
            "precio":self.precios,
            "ubicacion":self.ubicacion,
            "n_dormitorios":self.n_dormitorios,
            "n_banos":self.n_banos,
            "superficie_total": self.superficie_total,
            "superficie_util": self.superficie_util,
            "latitud":self.latitud,
            "longitud":self.longitud,
            "dias_desde_publicacion":self.dias_desde_publicacion,
            "estacionamientos": self.estacionamientos,
            "bodegas":self.bodegas,
            "cantidad_pisos_edificio":self.cantidad_pisos,
            "piso_unidad":self.piso_unidad,
            "tipo_casa":self.tipo_casa,
            "orientacion":self.orientacion,
            "antiguedad":self.antiguedad,
            "link": self.cards_urls}

        self.df_results = pd.DataFrame(dict_df)
        current_time_str = datetime.now().strftime('%H%M_%d_%m_%y')
        self.df_results.to_excel(f"results_{self.tipo_operacion}_{self.type}_{current_time_str}.xlsx")

    def handle_draw(self, self_2, action, geo_json):
        """extract the features  from on_Draw and save into a list"""
        # save of picked point
        self.picked_pts_features.append(geo_json)

        self.extract_urls_from_main_page_geo()

        # # update progress bar
        WebScraperPortalInmobiliario.update_info_progress_tqdm_bar(self.bar_progress_get_data,
                                                                   new_len=len(self.cards_urls),
                                                                   new_text='Obtaining data...')
        self.get_data_from_urls()

        if self.save_data:
            self.save_results()

    @staticmethod
    def update_info_progress_tqdm_bar(bar, new_len, new_text):
        """
        update inside descriptor of tqdm bar
        :param bar: tqdm method
        :param new_len: new len
        :param new_text: new text
        """
        bar.total=new_len
        bar.reset(total=new_len)
        bar.set_description(new_text)
        bar.refresh()


    def map_picker(self):
        """
        allows the user to pick from global map a location to search in
        more themes: https://wiki.openstreetmap.org/wiki/Raster_tile_providers
        """

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

        if self.theme=="dark":

            self.main_interactive_map = Map(layers=[dark_map_layer,], center=self.center_map_coordinates, zoom=13)

        elif self.theme=="default":

            self.main_interactive_map = Map(center=self.center_map_coordinates, zoom=13)

        elif self.theme=="white":
            self.main_interactive_map = Map(layers=[white_map_layer,], center=self.center_map_coordinates, zoom=13)


        # self.main_interactive_map = Map(center=self.center_map_coordinates, zoom=13)

        draw_control = DrawControl()

        draw_control.on_draw(self.handle_draw)

        self.main_interactive_map.add(draw_control)
        display(self.main_interactive_map) # NOQA
        self.bar_progress_get_data = tqdm(total=100, desc="SELECT REGION TO ANALYZE ...")

if __name__ == "__main__":

    # execute on jupyter only because of the dinamic map
    WebScraperPortalInmobiliario(tipo_operacion="venta",
                                 tipo_inmueble="casa",
                                 save_data=True,
                                 theme="default")

