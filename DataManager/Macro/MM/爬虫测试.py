import httpx
from bs4 import BeautifulSoup

global Authorization_value
Authorization_value = None


def get_MM_catalog():
    url = 'https://sc.macromicro.me/trader-insights'
    r = httpx.get(url)
    r.content.decode('utf8')
    soup = BeautifulSoup(r.content, 'html.parser')
    url_dict = {}
    for i in soup.find_all(class_='title'):
        url_dict[i.text] = i.a.attrs['href']
    return url_dict
