import requests
import pymongo
import json

API_URL = 'https://api.coinmarketcap.com/v2/ticker/?limit=20&sort=rank&structure=array'


def get_db_connection(uri):
    client = pymongo.MongoClient(uri)
    return client.cryptongo


def get_cryptocurrencies_from_api():
    r = requests.get(API_URL)
    if r.status_code == 200:
        result = json.loads(r.text)
        # print(json.dumps(result['data'], indent=2))

        return result
    raise Exception('API Error')


def first_element(elements):
    return elements[0]


def get_hash(value):
    from hashlib import sha512
    return sha512(value.encode('utf-8')).hexdigest()


def get_ticker_hash(ticker_data):
    from collections import OrderedDict
    ticker_data = OrderedDict(
        sorted(
            ticker_data.items(),
            key=first_element
        )
    )
    ticker_value = ''
    for _, value in ticker_data.items():
        ticker_value += str(value)
    return get_hash(ticker_value)


def check_if_exists(db_connection, ticker_data):
    ticker_hash = get_ticker_hash(ticker_data)
    if db_connection.ticker.find_one({"ticker_hash": ticker_hash}):
        return True

    return False


def save_ticker(db_connection, ticker_data=None):
    if not ticker_data:
        return False
    if check_if_exists(db_connection, ticker_data):
        return False

    ticker_hash = get_ticker_hash(ticker_data)
    ticker_data['ticker_hash'] = ticker_hash
    db_connection.ticker.insert_one(ticker_data)
    return True



# codigo para ser llamado desde la consola
if __name__ == "__main__":
    connection = get_db_connection('mongodb://localhost:27017/')
    tickers = get_cryptocurrencies_from_api()

    dataT = tickers["data"]

    #-------- FOR SI NO ES ARRAY LA API https://api.coinmarketcap.com/v2/ticker/

    # for attr, value in dataT.items():
    #     print(json.dumps(value,indent=2))
    #     save_ticker(connection,value)

    #-------- FOR SI ES ARRAY LA API https://api.coinmarketcap.com/v2/ticker/?limit=20&sort=rank&structure=array

    for value in dataT:
        print(json.dumps(value,indent=2))
        save_ticker(connection,value)


    print('done')