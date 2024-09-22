import pyppeteer
from pyppeteer import launch
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
import pandas as pd
import numpy as np
import ast
import random
from time import sleep

class Browser:
    def __init__(self):
        self.browser = None
        self.page = None
        self.total_number_request = 0
        self.ip_status_index = 0
        self.previus_ip_index = 0

    async def init_webdriver(self, get_images=False):
        """ Initialize Pyppeteer browser """
        ua = UserAgent()
        self.browser = await launch(headless=True, args=['--incognito'])
        self.page = await self.browser.newPage()

        # Set user agent
        await self.page.setUserAgent(ua.random)

        # Skip images if required
        if not get_images:
            await self.page.setRequestInterception(True)
            self.page.on('request', lambda req: asyncio.ensure_future(
                req.continue_() if req.resourceType in ['document', 'xhr', 'fetch', 'script', 'stylesheet'] else req.abort()
            ))

    async def webdriver_request(self, url, wait=0, xpath_wait=None):
        """ Request using Pyppeteer """
        self.total_number_request += 1
        await self.page.goto(url)

        random_percentage = random.uniform(0, 0.8)
        scroll_position = await self.page.evaluate(f"document.body.scrollHeight * {random_percentage}")
        await self.page.evaluate(f"window.scrollTo(0, {scroll_position});")

        if wait > 0:
            await asyncio.sleep(wait)

        try:
            if xpath_wait is not None:
                random_percentage = random.uniform(0, 0.8)
                scroll_position = await self.page.evaluate(f"document.body.scrollHeight * {random_percentage}")
                await self.page.evaluate(f"window.scrollTo(0, {scroll_position});")
                await self.page.waitForXPath(xpath_wait, {'timeout': 10000})
        except:
            await self.close_webdriver()
            await self.init_webdriver(get_images=True)
            return await self.webdriver_request(url, wait, xpath_wait)

        content = await self.page.content()
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    async def webdriver_refresh(self, wait=0):
        """ Refresh Pyppeteer page """
        await self.page.reload()

        random_percentage = random.uniform(0, 0.8)
        scroll_position = await self.page.evaluate(f"document.body.scrollHeight * {random_percentage}")
        await self.page.evaluate(f"window.scrollTo(0, {scroll_position});")

        if wait > 0:
            await asyncio.sleep(wait)

        content = await self.page.content()
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    async def close_webdriver(self):
        """ Close Pyppeteer browser """
        await self.browser.close()
        self.total_number_request = 0

    async def init_browser(self):
        self.browser = await launch(headless=True)
        self.page = await self.browser.newPage()

    async def close_browser(self):
        await self.browser.close()

    async def get_total_number_of_properties_in_location(self, min_lat, max_lat, min_lon, max_lon, operacion, tipo):
        self.geo_url_main = f"https://www.portalinmobiliario.com/{operacion}/{tipo}/_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}"
        await self.page.goto(self.geo_url_main)
        await self.page.waitForSelector('body')
        content = await self.page.content()
        search_text = 'melidata("add", "event_data", {"vertical":"REAL_ESTATE","query":"","limit":100,"offset":0,"total":'
        search_text_index = content.find(search_text)

        while search_text_index == -1:
            await self.page.reload()
            await asyncio.sleep(2)
            content = await self.page.content()
            search_text_index = content.find(search_text)

        text_number = self.find_next_string(content, search_text).split(",")
        if text_number:
            n_real_state = int(text_number[0])
        else:
            n_real_state = 0

        self.total_number_of_properties_pages += np.ceil(n_real_state / 50).astype(int)
        self.n_properties_dict[tipo][operacion] = n_real_state

    async def extract_urls_from_main_page_geo(self):
        polygon_coordinates = np.asarray(self.picked_pts_features[0]["geometry"]["coordinates"][0])
        max_lon = np.max(polygon_coordinates[:, 0])
        min_lon = np.min(polygon_coordinates[:, 0])
        max_lat = np.max(polygon_coordinates[:, 1])
        min_lat = np.min(polygon_coordinates[:, 1])

        tipos_inmueble = ["casa", "departamento"]
        tipos_de_operacion = ["arriendo", "venta"]
        self.n_properties_dict = {}
        self.total_number_of_properties_pages = 0
        self.cards_urls = []

        for tipo in tipos_inmueble:
            self.n_properties_dict[tipo] = {}
            for operacion in tipos_de_operacion:
                await self.get_total_number_of_properties_in_location(min_lat, max_lat, min_lon, max_lon, operacion, tipo)

        total = self.total_number_of_properties_pages
        self.bar_update(total, "Extracting url's from main pages")

        for tipo in tipos_inmueble:
            for operacion in tipos_de_operacion:
                n_main_pages = np.ceil(self.n_properties_dict[tipo][operacion] / 50).astype(int)
                for i in range(1, n_main_pages + 1):
                    url_extra_string = "_Desde_" + str(int(np.floor((i - 1) / 2) * 100 + 1)) if i >= 3 else ""
                    geo_url = f"https://www.portalinmobiliario.com/{operacion}/{tipo}/{url_extra_string}_DisplayType_M_item*location_lat:{min_lat}*{max_lat},lon:{min_lon}*{max_lon}#{i}"

                    await self.page.goto(geo_url)
                    await self.page.waitForXPath("//div[@class='ui-search-map-list ui-search-map-list__item']", timeout=4000)
                    await self.get_urls_from_containers()
                    self.bar.update(1)

    async def get_data_real_state_table(self, content):
        search_text = '"Características del inmueble",'
        index = content.find(search_text)
        next_index = index + len(search_text)

        moving_index = 0
        open_bracket_index = 0
        close_bracket_index = -1
        while True:
            if content[next_index + moving_index] == "[":
                open_bracket_index = next_index + moving_index + 1

            if content[next_index + moving_index] == "]":
                close_bracket_index = next_index + moving_index
                break

            moving_index += 1
            if moving_index > 900:
                tbl = await self.page.xpath("//table[@class='andes-table']")
                if tbl:
                    table_html = await self.page.evaluate('(tbl) => tbl.outerHTML', tbl[0])
                    table_data = pd.read_html(table_html)[0]
                    if len(table_data) > 0:
                        return table_data
                break

        data_dict = content[open_bracket_index:close_bracket_index]
        data = ast.literal_eval(data_dict)

        final_data = []
        for value in data:
            final_data.append([value["id"], value["text"]])

        return pd.DataFrame(final_data)

    async def get_correct_soup_from_url(self, url):
        try:
            await self.page.goto(url)
        except Exception as e:
            self.ip_status_index += 1
            await self.close_browser()
            await self.init_browser()
            pass

        content = await self.page.content()
        iter_count = 0
        while self.check_if_location_not_avaliable(content) or not await self.check_if_table_properties_avaliable(content):
            iter_count += 1
            await self.page.reload()
            await asyncio.sleep(4)
            content = await self.page.content()

            if iter_count > 3:
                await self.close_browser()
                await self.init_browser()
                print("Restarting browser, from get correct soup")
                iter_count = 0

        sleep(random.uniform(0, 3))
        return content

    async def check_if_table_properties_avaliable(self, content):
        try:
            self.table_data = await self.get_data_real_state_table(content)
            return True
        except Exception as e:
            return False

# Example usage
async def main():
    scraper = RealEstateScraper()
    await scraper.init_browser()
    await scraper.extract_urls_from_main_page_geo()
    await scraper.close_browser()

asyncio.get_event_loop().run_until_complete(main())