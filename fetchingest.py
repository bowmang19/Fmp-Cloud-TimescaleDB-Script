import config
import json
import time
import fmpsdk
import pandas as pd
import psycopg2
from pgcopy import CopyManager

# Loads all of the symbols into a list to have data fetched for
def get_ticker_symbol():
    f = open('symbols.json',)
    data = json.load(f)
    symbols = []
    for i in data:
        symbols.append(i['symbol'])
    f.close()
    print("FETCHED SYMBOLS")
    return symbols

# Fetches the data for each symbol and stores in timescaledb
def fetch_stock_data(symbols, apikey, conn, COLUMNS):
    for i in range(len(symbols)):
        print("SYMBOL:", symbols[i], str(i) + "/" + str(len(symbols)))
        data = fmpsdk.historical_price_full(apikey=apikey, symbol=symbols[i])
        df = pd.DataFrame.from_dict(data['historical'])
        df = df.drop(['adjClose', 'unadjustedVolume', 'change', 'changePercent', 'vwap', 'label', 'changeOverTime'], axis = 1)
        df = df.rename(columns={'date':'time',
                                'open':'price_open',
                                'close':'price_close',
                                'high':'price_high',
                                'low':'price_low',
                                'volume':'trading_volume'})
        df['symbol'] = symbols[i]
        df = df.reindex(columns=COLUMNS)
        df = df.fillna(0)
        df = df.astype({'trading_volume':int})
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S')
        data = [row for row in df.itertuples(index=False, name=None)]
        mgr = CopyManager(conn, 'stocks', COLUMNS)
        mgr.copy(data)
        conn.commit()
    print("FINISHED")
    return

def main():
    apikey = config.APIKEY
    conn = psycopg2.connect(database = config.DB_NAME,
                            host = config.DB_HOST,
                            user = config.DB_USER,
                            password = config.DB_PASS,
                            port = config.DB_PORT)
    COLUMNS = ['time', 'symbol', 'price_open', 'price_close', 'price_low',
            'price_high', 'trading_volume']
    symbols = get_ticker_symbol()
    data = fetch_stock_data(symbols, apikey, conn, COLUMNS)


if __name__ == "__main__":
    main()