"""Скрипт предназанчен для автоматического обновленя цен на маркетплейсе Яндекс Маркет."""
import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из магазина Яндекс.Маркета.

    Функция обращается к API Яндекс.Маркета для получения списка товаров.

    Аргументы:
        page(str): Токен страницы.
        campaign_id(str): Идентификатор кампании.
        access_token(str): Токен доступа к API.

    Возвращаемое значение:
        dict: содержащий список товаров.
        Пример:
        {
            "result": {
                "offerMappingEntries": [
                    {"offer": {"shopSku": "123", "name": "Товар 1", ...}},
                    {"offer": {"shopSku": "456", "name": "Товар 2", ...}},
                ],
                "paging": {"nextPageToken": "token"}
            }
        }

    Исключения:
        requests.exceptions.RequestException: При ошибках сетевого запроса.
        HTTPError: При HTTP-ошибках (например, 400, 401, 500).
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров на Яндекс.Маркете.

    Функция отправляет запрос на обновление остатков товаров через API Яндекс.Маркета.

    Аргументы:
        stocks(list): Словарей, содержащих информацию об остатках товаров.
        campaign_id(str): Идентификатор кампании.
        access_token(str): Токен доступа к API.

    Возвращаемое значение:
        dict: с результатом обновления остатков.
        Пример:
        {'status': 'OK'}

    Исключения:
        requests.exceptions.RequestException: При ошибках сетевого запроса.
        HTTPError: При HTTP-ошибках (например, 400, 401, 500).
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
     """Обновляет цены товаров на Яндекс.Маркете.

    Функция отправляет запрос на обновление цен товаров через API Яндекс.Маркета.

    Аргументы:
        prices(list): словарей, содержащих информацию о ценах товаров.
        campaign_id(str): Идентификатор кампании.
        access_token(str): Токен доступа к API.

    Возвращаемое значение:
        dict: с результатом обновления цен.
        Пример:
        {'status': 'OK'}

    Исключения:
        requests.exceptions.RequestException: При ошибках сетевого запроса.
        HTTPError: При HTTP-ошибках (например, 400, 401, 500).
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает список артикулов товаров из Яндекс.Маркета.

    Функция получает список offer_id товаров из Яндекс.Маркета.

    Аргументы:
        campaign_id(str): Идентификатор кампании.
        market_token(str): Токен доступа к API Яндекс.Маркета.

    Возвращаемое значние:
        list: артикулов товаров (shopSku).
        Пример:
        ['123', '456', '789']
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает список остатков для отправки на Яндекс.Маркет.

    Функция формирует список словарей с остатками для отправки на Яндекс.Маркет.

    Аргументы:
        watch_remnants(list): словарей с остатками часов.
        offer_ids(list): артикулов товаров на Яндекс.Маркет.
        warehouse_id(str): Идентификатор склада.

    Возвращаемое значение:
        list: словарей, содержащих информацию об остатках для отправки на Яндекс.Маркет.
        Пример:
        [
            {'sku': '123', 'warehouseId': 1234, 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2023-10-27T12:00:00Z'}]},
            {'sku': '456', 'warehouseId': 1234, 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2023-10-27T12:00:00Z'}]},
        ]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для отправки на Яндекс.Маркет.

    Функция формирует список словарей с ценами для отправки на Яндекс.Маркет.

    Аргументы:
        watch_remnants(list): словарей с остатками часов.
        offer_ids(list): артикулов товаров на Яндекс.Маркет.

    Возвращаемое значение:
        Со словарями, содержащих информацию о ценах для отправки на Яндекс.Маркет.
        Пример:
        [
            {'id': '123', 'price': {'value': 5000, 'currencyId': 'RUR'}},
            {'id': '456', 'price': {'value': 10000, 'currencyId': 'RUR'}},
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загружает цены товаров на Яндекс.Маркет асинхронно.

    Функция асинхронно получает offer_id, создает список цен и загружает их на Яндекс.Маркет.

    Аргументы:
        watch_remnants(list): Список словарей с остатками часов.
        campaign_id(str): Идентификатор кампании.
        market_token(str): Токен доступа к API Яндекс.Маркета.

    Возвращаемое значение:
        Список словарей, содержащих информацию о ценах, которые были загружены.
        Пример:
        [
            {'id': '123', 'price': {'value': 5000, 'currencyId': 'RUR'}},
            {'id': '456', 'price': {'value': 10000, 'currencyId': 'RUR'}},
        ]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружает остатки товаров на Яндекс.Маркет асинхронно.

    Функция асинхронно получает offer_id, создает список остатков и загружает их на Яндекс.Маркет.

    Аргументы:
        watch_remnants(list): Список словарей с остатками часов.
        campaign_id(str): Идентификатор кампании.
        market_token(str): Токен доступа к API Яндекс.Маркета.
        warehouse_id(str): Идентификатор склада.

    Возвращаемое значение:
        tulip: содержащий:
            - Список словарей, содержащих информацию об остатках, у которых count не равен 0.
            - Полный список словарей с информацией об остатках.
        Пример:
        ([{'sku': '123', 'warehouseId': 1234, 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2023-10-27T12:00:00Z'}]}], 
         [{'sku': '123', 'warehouseId': 1234, 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2023-10-27T12:00:00Z'}]},
          {'sku': '456', 'warehouseId': 1234, 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-10-27T12:00:00Z'}]}])
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
