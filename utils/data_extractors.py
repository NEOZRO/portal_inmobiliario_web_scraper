
import json
import re
import numpy as np

class DataExtractor:
    """ class for extracting data from website/soups """
    def __init__(self):

        self.list_operations = []
        self.list_tipos_inmueble = []
        self.current_type_operation = None
        self.dict_len_type_operations = None
        self.GC = None
        self.antiguedad = None
        self.orientacion = None
        self.tipo_inmueble = None
        self.piso_unidad = None
        self.cantidad_pisos = None
        self.bodegas = None
        self.estacionamientos = None
        self.n_banos = None
        self.n_dormitorios = None
        self.superficie_util = None
        self.superficie_total = None
        self.dias_desde_publicacion = None
        self.precios = None
        self.longitud = None
        self.latitud = None
        self.title = None
        self.total_dict_properties = None
        self.table_data = None
        self.usd_clp = None
        self.uf = None
        self.json_polygon_path = None
        self.main_soup = None
        self.picked_pts_features = None
        self.cards_urls = []
        self.geo_url_main = None
        self.type = None
        self.tipo_operacion = None



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


    def get_urls_from_containers(self):
        """
        get all cards urls from each containers/cards list
        """

        container_list = self.main_soup.find_all('div', {'class': 'ui-search-map-list ui-search-map-list__item'})
        for container in container_list:
            url = container.find('a', class_='ui-search-result__main-image-link ui-search-link')['href']
            self.cards_urls.append(url)
            self.list_tipos_inmueble.append(self.type)
            self.list_operations.append(self.tipo_operacion)


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

    def get_data_from_table_in_url(self):
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
            "ui-pdp-color--GRAY ui-pdp-size--XXSMALL ui-pdp-family--REGULAR ui-pdp-seller-validated__title",
            "ui-pdp-subtitle ui-pdp-subtitle_rex",
            "ui-pdp-background-color--LIGHT_GRAY ui-pdp-color--GRAY ui-pdp-size--XSMALL ui-pdp-family--SEMIBOLD ui-pdp-header__bottom-subtitle"]
        list_types_publications_days = [
            "p",
            "p",
            "p",
            "p",
            "span",
            "p"]
        index_grabber = 0

        while True: # make break condition and a break
            if index_grabber < len(list_grabbers_publications_days):
                days_publication_line = soup.find_all(list_types_publications_days[index_grabber],
                                                      list_grabbers_publications_days[index_grabber])
                if days_publication_line:
                    if  days_publication_line[0].text.find("publicado") != -1 or days_publication_line[0].text.find("Publicado") != -1:
                        if  days_publication_line[0].text.find("hoy") != -1:
                            period = "dia"
                            quantity = 1
                        elif days_publication_line[0].text.find("semana") != -1:
                            period = "dia"
                            quantity = 7
                        else:
                            quantity = int(self.find_next_string(days_publication_line[0].text, "hace"))
                            period = self.find_next_string(days_publication_line[0].text, str(quantity))
                        break
                    else:
                        index_grabber += 1
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

    def check_if_location_not_avaliable(self, soup):
        """ check if location loaded correctly """
        if soup is not None:
            len_locations_found = len(soup.find_all('div', {"id": "ui-vip-location__map"}))
            return len_locations_found == 0
        else:
            return True

    def init_dict_properties(self):
        """
        initialize dict with all the properties as nan
        """
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

    def append_all_propierties(self,title,latitud,longitud,price_value,n_days_since_published):
        """
        append all properties to self.total_dict_properties
        """
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

    def get_main_properties_from_soup(self, soup):
        """ get basic properties from soup """

        title = soup.find_all("h1", "ui-pdp-title")[0].text
        price_value = self.get_price_from_soup(soup)
        n_days_since_published = self.get_days_since_published(soup)
        latitud, longitud = self.get_latitud_longitud_from_soup(soup)

        return title, latitud, longitud, price_value, n_days_since_published

    def init_main_properties(self):
        """init empty/nan properties"""

        latitud = np.NAN
        longitud = np.NAN
        price_value = np.NAN
        n_days_since_published = np.NAN
        title = np.NAN

        return title, latitud, longitud, price_value, n_days_since_published

    def save_soup_as_json(self, soup):
        """save soup as json"""
        with open("soup.json", "w", encoding="utf-8") as file:
            # BEAUTIFUL JSON BEFORE SAVE
            soup = soup.prettify()
            json.dump(str(soup), file, ensure_ascii=False, indent=4)
