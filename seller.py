"""Скрипт предназначен для автоматического обновления цен на маркетплейсе Озон."""

import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)



def get_product_list(last_id, client_id, seller_token):
    """Возвращает список товаров магазина озон.
    Функция принимает три аргумента last_id, клиентское ID и токен продавца.
    Для этого, функция обращается к api озон, передавая свои аргументы.
    
    Аргументы:
        last_id(str): Идентификатор последнего товара.
        client_id(str): Идентификатор клиента Ozon.
        seller_token(str): Токен продавца Ozon.

    Возвращаемое значение:
        dict: содержащий список товаров.
        Примеры:
        >>>get_product_list('wrong_last_id','wrong_client_id', 'wrong_seller_tokken')
        {
            "items": [
                {"offer_id": "123", "name": "Товар 1", ...},
                {"offer_id": "456", "name": "Товар 2", ...},
            ],
            "total": 100,
            "last_id": "789"
        }

        >>>get_product_list('wrong_last_id','wrong_client_id', 'wrong_seller_tokken')
            
    Исключения:
        HTTPError: При HTTP-ошибках (например, 400, 401, 500).    
        requests.exceptions.RequestException: При ошибках сетевого запроса.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    respons_oebject = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Возвращает артикулы товаров магазина озон.
    Функция принимает два аргумента клиентское ID и токен продавца, создает список артикулов товаров магазина озон. 
    Функция использует функцию get_product_list для получения списка товаров магазина озон, передавая свои аргументы и аргумент last_id.

    Аргументы:
        client_id(str): Идентификатор клиента Ozon.
        seller_token(str): Токен продавца Ozon.

    Возвращаемое значение:
        list: со строками, где каждая строка это offer_id.

        Примеры:
        >>> get_offer_ids('your_client_id', 'your_seller_token')
        '*Артикул товара№1*', '*Артикул товара№2*', ...*'
    
        >>> get_offer_ids('empty_client_id', 'empty_seller_token')
        []
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров на Ozon.

    Функция принимает список цен и учетные данные Ozon и отправляет запрос на обновление цен через API.

    Аргументы:
        prices(list): со словарями, содержащих информацию о ценах товаров.
        client_id(str): Идентификатор клиента Ozon.
        seller_token(str): Токен продавца Ozon.

    Возвращаемое значение:
        dict: с результатом обновления цен.

    Примеры:
        >>> update_price([{'offer_id': '123', 'price': '1000'}], 'your_client_id', 'your_seller_token')
        {'result': 'ok'}

    Исключения:
        requests.exceptions.RequestException: При ошибках сетевого запроса.
        HTTPError: При HTTP-ошибках (например, 400, 401, 500).
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на Ozon.

    Функция принимает список остатков и учетные данные Ozon и отправляет запрос на обновление остатков через API.

    Аргументы:
        stocks(list): со словарями, содержащих информацию об остатках товаров.
        client_id(str): Идентификатор клиента Ozon.
        seller_token(str): Токен продавца Ozon.

    Возвращаемое значение:
        dict: с результатом обновления остатков.

    Примеры:
        >>> update_stocks([{'offer_id': '123', 'stock': 10}], 'your_client_id', 'your_seller_token')
        {'result': 'ok'}

    Исключения:
        requests.exceptions.RequestException: При ошибках сетевого запроса.
        HTTPError: При HTTP-ошибках (например, 400, 401, 500).
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл остатков с сайта Casio и возвращает список остатков часов.

    Функция загружает ZIP-архив с остатками, извлекает Excel-файл, 
    обрабатывает его и возвращает список словарей, содержащих информацию об остатках.

    Возвращаемое значение:
        list: со словарями, где каждый словарь представляет остаток товара.
        Пример:
        [
            {'Код': '123', 'Количество': 10, 'Цена': '5000.00'},
            {'Код': '456', 'Количество': '>10', 'Цена': '10000.00'},
        ]

    Исключения:
        requests.exceptions.RequestException: При ошибках скачивания файла.
        zipfile.BadZipFile: Если загруженный файл не является корректным ZIP-архивом.
        pandas.errors.ParserError: Если Excel-файл имеет неправильный формат.
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков для отправки на Ozon.

    Функция принимает список остатков часов и список offer_id товаров на Ozon, 
    формирует список словарей с остатками для отправки на Ozon.

    Аргументы:
        watch_remnants(list): Со словарями с остатками часов.
        offer_ids(list): offer_id товаров на Ozon.

    Возвращаемое значение:
        list: со словарями, содержащих информацию об остатках для отправки на Ozon.
        Пример:
        [
            {'offer_id': '123', 'stock': 10},
            {'offer_id': '456', 'stock': 100},
        ]
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для отправки на Ozon.

    Функция принимает список остатков часов и список offer_id товаров на Ozon, 
    формирует список словарей с ценами для отправки на Ozon.

    Аргументы:
        watch_remnants(list): Cо словарями с остатками часов.
        offer_ids(list): offer_id товаров на Ozon.

    Возвращаемое значние:
        list: со словарями, содержащих информацию о ценах для отправки на Ozon.
        Пример:
        [
            {'offer_id': '123', 'price': '5000', 'currency_code': 'RUB', 'old_price': '0', 'auto_action_enabled': 'UNKNOWN'},
            {'offer_id': '456', 'price': '10000', 'currency_code': 'RUB', 'old_price': '0', 'auto_action_enabled': 'UNKNOWN'},
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку, представляющую цену, в целое число.

    Функция принимает строку, содержащую цену в формате "целая_часть.дробная_часть",
    удаляет все символы, кроме цифр, и возвращает строку, содержащую только целую часть цены.
    
    Аргументы:
        str: цену для преобразования, строка.

    Возвращаемое значение:
        str: Преобразованная строка.
        Примеры:
        >>> price_conversion('5990.00 руб.')
        '5990'
        >>> price_conversion('3400.00')
        '3400'

        Не правильно: price_conversion(3400.00)
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части по n элементов.

    Функция принимает список и число n и возвращает генератор, 
    который разбивает список на части по n элементов.

    Аргументы:
        lst(list): Список для разделения.
        n(int): Размер каждой части.

    Возвращаемое значие:
        list: генератор, возвращающий части списка.
        Пример:
        >>> list(divide([1, 2, 3, 4, 5, 6], 2))
        [[1, 2], [3, 4], [5, 6]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает цены товаров на Ozon асинхронно.

    Функция асинхронно получает offer_id, создает список цен и загружает их на Ozon, 
    используя пагинацию для отправки больших списков.

    Аргументы:
        watch_remnants(list): Список словарей с остатками часов.
        client_id(str): Идентификатор клиента Ozon.
        seller_token(str): Токен продавца Ozon.

    Возвращаемое значие:
        list: со словарями, содержащих информацию о ценах, которые были загружены.
        Пример:
        [
            {'offer_id': '123', 'price': '5000', 'currency_code': 'RUB', 'old_price': '0', 'auto_action_enabled': 'UNKNOWN'},
            {'offer_id': '456', 'price': '10000', 'currency_code': 'RUB', 'old_price': '0', 'auto_action_enabled': 'UNKNOWN'},
        ]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает остатки товаров на Ozon асинхронно.

    Функция асинхронно получает offer_id, создает список остатков и загружает их на Ozon, 
    используя пагинацию для отправки больших списков.

    Аргументы:
        watch_remnants(list): Со словарями с остатками часов.
        client_id(str): Идентификатор клиента Ozon.
        seller_token(str): Токен продавца Ozon.

    Возвращаемое значение:
        tulip: содержащий:
            - Список словарей, содержащих информацию об остатках, у которых stock не равен 0.
            - Полный список словарей с информацией об остатках.
        Пример:
        ([{'offer_id': '123', 'stock': 10}], [{'offer_id': '123', 'stock': 10}, {'offer_id': '456', 'stock': 0}])
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
