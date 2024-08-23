import requests, re
from bs4 import BeautifulSoup

def get_free_proxies():
    """
    Gets free proxies from spys.me
    :return: list of valid proxies
    """

    regex = r"[0-9]+(?:\.[0-9]+){3}:[0-9]+"
    c = requests.get("https://spys.me/proxy.txt")
    test_str = c.text
    a = re.finditer(regex, test_str, re.MULTILINE)
    with open("proxies_list.txt", 'w') as file:
        for i in a:
           print(i.group(),file=file)

    d = requests.get("https://free-proxy-list.net/")
    soup = BeautifulSoup(d.content, 'html.parser')
    td_elements = soup.select('.fpl-list .table tbody tr td')
    ips = []
    ports = []
    total_proxies=[]
    for j in range(0, len(td_elements), 8):
        ip = td_elements[j].text.strip()
        port = td_elements[j + 1].text.strip()
        ips.append(ip)
        ports.append(port)
        total_proxies.append(f"{ip}:{port}")

    return total_proxies
