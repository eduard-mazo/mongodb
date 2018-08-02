import requests
import pymongo

API_URL = 'https://api.coinmarketcap.com/v2/ticker/'


def get_db_connection(uri):
    client = pymongo.MongoClient(uri)
    return client.cryptongo


def get_cryptocurrencies_from_api():
    r = requests.get(API_URL)
    if r.status_code == 200:
        result = r.json()
        return result
    raise Exception('API Error')


def check_if_exists(db_connection, ticker_data):

    if db_connection.find_one({"ticker_hash"}):
        return True

    return False


def save_ticker(db_connection, ticker_data=None):
    if not ticker_data:
        return False
    if check_if_exists(db_connection, ticker_data):
        return False

    ticker_data['rank'] = int(ticker_data['rank'])
    ticker_data['last_updated'] = int(ticker_data['last_updated'])

    db_connection.ticker.insert_one(ticker_data)
    return True
