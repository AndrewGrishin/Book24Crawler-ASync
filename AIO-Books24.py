import aiohttp
import asyncio
import requests
import sys
import json
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
from math import ceil
from tqdm.auto import tqdm

MAIN_URL = 'https://book24.ru'
SCHEME = 'https://book24.ru/search/page-{}/?q={}'
SELECTORS = {
    'get_page_number': {
        'get_found_product_number': '.search-page__desc',
        'get_found_product_number_per_page': '.product-list.catalog__product-list > .product-list__item'
    },
    'get_books_url_one_page': {
        'get_found_books_urls': '.product-list__item .product-card__image-holder > a'
    },
    'get_book_info': {
        'Название': '.breadcrumbs.product-detail-page__breadcrumbs .breadcrumbs__item._last-item',
        'Описание': '.product-about__text',
        'Цена': '.app-price.product-sidebar-price__price',
        'Артикул': '.product-detail-page__article',
        'Характеристики': '#product-characteristic .product-characteristic__item'
    }
}

# Get page numbers
def get_resp(url: str, params: dict=None) -> requests.Response:
    params = params if not (params is None) else dict()
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    resp = requests.get(url, headers=headers, params=params)
    return resp

def get_soup(resp: requests.Response) -> BeautifulSoup:
    soup = BeautifulSoup(resp.text if not isinstance(resp, str) else resp, 'lxml')
    return soup

def get_page_number(url: str, selectors: str) -> int:
    resp = get_resp(url)
    soup = get_soup(resp)
    num_of_products = soup.select_one(selectors['get_found_product_number'])
    num_of_products = num_of_products.get_text()
    num_of_products_total = re.findall(re.compile(r'[0-9]+'), num_of_products)[0]
    num_of_products_per_page = len(soup.select(selectors['get_found_product_number_per_page']))
    return ceil(int(num_of_products_total) / num_of_products_per_page)
# end 

# Async: books links (from previews)
async def get_page_html(session, url: str, selectors: dict, main_url: str) -> list:
    async with session.get(url) as resp:
        text = await resp.text()
        soup = BeautifulSoup(text, 'lxml')
        transform = lambda x: main_url + x.get('href')
        tags = soup.select(selectors['get_found_books_urls'])
        tags = list(map(transform, tags))
        return tags

async def get_pages_urls(main_url: str, scheme: str, query: str, num_of_pages: int, selectors: dict):
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        pbar = tqdm(list(range(num_of_pages)), desc='Page', ncols=85)
        for page in pbar:
            task = asyncio.create_task(get_page_html(
                session, scheme.format(page + 1, query), selectors, main_url))
            await asyncio.sleep(0.05)
            tasks.append(task)
        data = await asyncio.gather(*tasks)
    data = [html for page in data for html in page]
    return data
# end 

# Async: books' page html
async def get_book_html(session, url: str) -> str:
    async with session.get(url) as resp:
        return await resp.text()

async def get_books_htmls(urls: list) -> list:
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as session:
        pbar = tqdm(urls, desc='URL', ncols=85)
        for url in pbar:
            task = asyncio.create_task(get_book_html(session, url))
            await asyncio.sleep(0.05)
            tasks.append(task)
        books_htmls = await asyncio.gather(*tasks)
        return books_htmls
# end

# Sync: books data
def get_book_data(html: str, selectors: dict) -> dict:
    soup = get_soup(html)
    book_data = {key: None for key in selectors.keys()}
    try: 
        book_data['Название'] = soup.select_one(selectors['Название'])
        book_data['Название'] = book_data['Название'].get_text().strip()
    except:
        pass

    try: 
        book_data['Описание'] = soup.select_one(selectors['Описание'])
        book_data['Описание'] = book_data['Описание'].get_text().strip()
    except:
        pass

    try: 
        book_data['Цена'] = soup.select_one(selectors['Цена']) 
        book_data['Цена'] = book_data['Цена'].get_text().strip().replace('\xa0', ' ')
    except:
        pass

    try: 
        book_data['Артикул'] = soup.select_one(selectors['Артикул'])
        book_data['Артикул'] = book_data['Артикул'].get_text().strip().split(' ')[1]
    except:
        pass 

    try: 
        characteristics = soup.select(selectors['Характеристики'])
        book_data['Характеристики'] = dict()
        for el in characteristics:
            key = el.select_one('.product-characteristic__label-holder').get_text().strip()[:-1]
            try:
                val = el.select_one('.product-characteristic__value').get_text().strip()
                book_data['Характеристики'][key] = val
            except:
                book_data['Характеристики'][key] = None
    except:
        pass

    return book_data

def get_books_data(htmls, selectors: dict) -> list:
    data = []
    pbar = tqdm(htmls, desc='Book', ncols=85)
    for html in pbar:
        data.append(get_book_data(html, selectors))
    return data
# end

if __name__ == '__main__':
    query = ' '.join(sys.argv[1:])
    # get number of pages
    number_of_pages = get_page_number(SCHEME.format(1, query), selectors=SELECTORS['get_page_number'])
    print(f'Number of pages: {number_of_pages}.')
    # get books links
    print(f'Books links gatherment...')
    books_urls = asyncio.run(get_pages_urls(MAIN_URL, SCHEME, query, number_of_pages, selectors=SELECTORS['get_books_url_one_page']))
    print('Gathered books links: ' + str(len(books_urls)) + '.')
    books_htmls = asyncio.run(get_books_htmls(books_urls))
    print('Gathered books htmls: ' + str(len(books_htmls)) + '.')
    books_data = get_books_data(books_htmls, SELECTORS['get_book_info'])
    print(f'Gathered books info: {len(books_data)}.')
    file_name = query.replace("&", "_").replace(" ", "_")
    with open(f'{file_name}.json', 'w') as file:
        json.dump(books_data, file, indent=4, ensure_ascii=False)
    print(f'File `{file_name}.json` created!')

