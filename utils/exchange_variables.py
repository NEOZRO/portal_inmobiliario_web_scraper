
import requests
from bs4 import BeautifulSoup
import numpy as np

class ExchangeVariables:
    """ class for scrapping exchange variables/constants from website/soups """

    def __init__(self):
        self.usd_clp = None
        self.uf = None

    def get_uf_today(self):
        """
        return unit of UF of today to make convertion of prices later
        Sometimes at night the website is  updated with the UF value  of zero, affecting the price convertions
        """
        url_uf = "https://valoruf.cl/"
        type = "span"
        id = 'vpr'
        response = requests.get(url_uf)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            div = soup.find(type, {'class': id})

            integer = int(div.get_text().split(",")[0].split("$")[1].replace(".", ""))
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
