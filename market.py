import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров из Яндекс Маркета.

    Args:
        page (int): Номер страницы.
        campaign_id (str): ID логистического метода (FBS, DBS).
        access_token (str): Токен для работы с API Яндекс Маркет.

    Returns:
        dict: Возвращает словарь со товарами.

    Example:
        Пример, с корректным использованием:
        >>> get_product_list(5, 'campaign_id', 'access_token'):
        "result": {}
        Пример, с некорректным использованием:
        >>> get_product_list(5, 'campaign_id', '<invalid_access_token>'):
        Token Error: Invalid token
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
    """Обновить остатки.

    Args:
        stocks (list): Остатки выгруженные из сайта Casio.
        campaign_id (str): ID логистического метода (FBS, DBS).
        access_token (str): Токен для работы с API Яндекс Маркет.

    Returns:
        dict: Возвращает статус ответа

    Example:
        Пример, с корректным использованием:
        >>> update_stocks(stocks, 'campaign_id', 'access_token'):
        {"status": "OK"}
        Пример, с некорректным использованием:
        >>> update_stocks(stocks, 'invalid_campaign_id', 'access_token'):
        ID Error: Ivalid campaign id
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
    """Обновить цену.

    Args:
        prices (list): Список с ценой из остатков.
        campaign_id (str): ID логистического метода (FBS, DBS).
        access_token (str): Токен для работы с API Яндекс Маркет.

    Returns:
        dict: Возвращает статус ответа

    Example:
        Пример, с корректным использованием:
        >>> update_price(prices, 'campaign_id', 'access_token'):
        {"status": "OK"}
        Пример, с некорректным использованием:
        >>> update_price(prices, 'campaign_id', 'access_token'):
        NameError: name 'prices' is not defined

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
    """Получить артикулы товаров Яндекс маркета.

    Args:
        campaign_id (str): ID логистического метода (FBS, DBS).
        market_token (str): Токен для работы с API Яндекс Маркет.

    Returns:
        list: Возвращает список с артикулами товаров.

    Example:
        Пример, с корректным использованием:
        >>> get_offer_ids('campaign_id', 'market_token'):
        ['offer_ids']
        Пример, с некорректным использованием:
        >>> get_offer_ids('campaign_id', '<invalid_market_token>'):
        Token Error: Invalid token
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
    """Создает список с остатками товаров.

    Args:
        watch_remnants (list): Остатки выгруженные из сайта Casio.
        offer_ids (list): Список с артикулами товаров.
        warehouse_id (str): ID склада (FBS, DBS)

    Returns:
        list: Возвращает список с остаткамими товаров.

    Example:
        Пример, с корректным использованием:
        >>> create_stocks(watch_remnants, offer_ids):
        ['item1', 'item2']
        Пример, с некорректным использованием:
        >>> create_stocks(watch_remnants, offer_ids):
        Вызывает ошибку, если один из параметров некорректен.
    """
    # Уберем то, что не загружено в market
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
    """Формирует список цен для предложений, имеющихся в `offer_ids`.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список с артикулами.

    Returns:
        list[dict]: Возвращает список словарей с ценами.

    Example:
        Пример, с корректным использованием:
        >>> create_prices(watch_remnants, offer_ids):
        [{'id': '123', 'price': {'value': 1000, 'currencyId': 'RUR'}}]
        Пример, с некорректным использованием:
        >>> create_prices(watch_remnants, offer_ids):
        Traceback (most recent call last):
        Ошибка из-за некорректного типа данных
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
    """Загружает обновления цен частями по 500 для указанного метода (FBS, DBS)."""
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружает обновления остатков частями по 2000 и возвращает ненулевые остатки."""
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
