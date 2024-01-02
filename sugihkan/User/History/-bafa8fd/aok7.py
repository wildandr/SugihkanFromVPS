from datetime import datetime, timedelta
import os
import time
import pandas as pd
import yfinance as yf

def update_market_data(symbol):
    directory = "/root/sugihkan/forex"
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = f"{directory}/{symbol.replace('/', '')}_data.csv"
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
        if not existing_data.empty:
            last_date_str = existing_data['Datetime'].max()
            last_date = pd.to_datetime(last_date_str)
            last_date = last_date.replace(tzinfo=None)
            if last_date > start_date:
                start_date = last_date + timedelta(minutes=1)
    else:
        existing_data = pd.DataFrame()

    if start_date < end_date:
        new_data = yf.download(symbol, start=start_date, end=end_date, interval='1m')
        if not new_data.empty:
            new_data.reset_index(inplace=True)
            new_data.rename(columns={'index': 'Datetime'}, inplace=True)
            updated_data = pd.concat([existing_data, new_data])
            updated_data.to_csv(file_path, index=False)

    return file_path

symbols = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X",
    "AUDUSD=X", "USDCAD=X", "NZDUSD=X", "AUDCAD=X",
    "AUDCHF=X", "AUDJPY=X", "AUDNZD=X", "EURAUD=X",
    "EURCAD=X", "EURJPY=X", "EURNZD=X", "GBPAUD=X",
    "GBPCAD=X", "GBPCHF=X", "GBPJPY=X", "GBPNZD=X",
    "NZDCAD=X", "NZDJPY=X", "XAUUSD=X", "XAGUSD=X", 
    "OIL"
]

while True:
    for symbol in symbols:
        update_market_data(symbol)
    time.sleep(60)  # Wait for one minute before next update
