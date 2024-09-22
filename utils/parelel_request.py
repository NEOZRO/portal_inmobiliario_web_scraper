import asyncio
import aiohttp
import random
import socket
from bs4 import BeautifulSoup
from data_extractors import DataExtractor
super_proxy = socket.gethostbyname('brd.superproxy.io')

LISTA_URLS_TEST = ['https://www.portalinmobiliario.com/MLC-1526182233-casa-calle-matias-cousino-id-139596-_JM#position%3D1%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-2696968916-casa-en-arriendo-en-penalolen-_JM#position%3D2%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-1526035915-cerca-metro-los-presidentes-reciento-cerrado-muy-seguro-_JM#position%3D3%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-1523679829-macul-gran-casa-recien-remodelada-_JM#position%3D4%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-2676088274-casa-de-4-dormitorios-en-hacienda-macul-penalolen-_JM#position%3D5%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-2643529278-casa-en-condominio-con-piscina-_JM#position%3D6%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-2700972302-espectacular-casa-en-arriendo-de-3d3b-en-penalolen-_JM#position%3D7%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-1525711647-portal-de-la-vina-34440-_JM#position%3D8%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-2689604054-arriendo-casa-44523-_JM#position%3D9%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1',
                   'https://www.portalinmobiliario.com/MLC-1517658583-casa-2-pisos-154m24d3b2-est-penalolen-38899-_JM#position%3D10%26search_layout%3Dmap%26type%3Ditem%26tracking_id%3D857ad591-cae9-4fec-a1f2-c3b33448add1']
class SingleSessionRetriever:
    url = "http://%s-session-%s:%s@"+super_proxy+":%d"
    port = 22225

    def __init__(self, username, password):
        self._username = username
        self._password = password
        self._reset_session()

    def _reset_session(self):
        session_id = str(random.random())
        self._proxy = self.url % (self._username, session_id, self._password, SingleSessionRetriever.port)

    async def retrieve(self, url, timeout):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, proxy=self._proxy, timeout=timeout, headers=headers) as response:
                    return await response.text()
            except Exception as e:
                print(f"Request failed: {e}, Type: {type(e).__name__}")
                return None


class MultiSessionRetriever:
    def __init__(self, username, password, session_requests_limit, session_failures_limit):
        self._username = username
        self._password = password
        self.session_requests_limit = session_requests_limit
        self.session_failures_limit = session_failures_limit
        self._sessions_stack = []
        self._requests = 0

    async def retrieve(self, urls, timeout, parallel_sessions_limit, callback):
        semaphore = asyncio.Semaphore(parallel_sessions_limit)
        tasks = [self._retrieve_single(url, timeout, semaphore, callback) for url in urls]
        await asyncio.gather(*tasks)

    async def _retrieve_single(self, url, timeout, semaphore, callback):
        async with semaphore:
            if not self._sessions_stack or self._requests >= self.session_requests_limit:
                if self._sessions_stack:
                    self._requests = 0
                session_retriever = SingleSessionRetriever(self._username, self._password)
                self._sessions_stack.append(session_retriever)
            else:
                session_retriever = self._sessions_stack[-1]
            self._requests += 1
            body = await session_retriever.retrieve(url, timeout)
            if body is not None:
                await callback(url, body)

async def output(url, body):
    # print(f"URL: {url}, Body: {body[:1000]}...")
    print("URL: ", url)
    soup = BeautifulSoup(body, 'html.parser')
    days = DataExtractor().get_days_since_published(soup) # todo wont work because of javascript wont fully load unless in a real browser
    print("days: ", days)
    print("----------------")


async def main():
    req_timeout = 30
    n_parallel_exit_nodes = 10
    switch_ip_every_n_req = 1
    max_failures = 2

    retriever = MultiSessionRetriever('brd-customer-hl_aaf60d4d-zone-residential_proxy_x', '0r3sudcgsaud', switch_ip_every_n_req, max_failures)
    await retriever.retrieve(LISTA_URLS_TEST , req_timeout, n_parallel_exit_nodes, output)

if __name__ == '__main__':
    asyncio.run(main())