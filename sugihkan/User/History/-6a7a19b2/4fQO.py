from datetime import datetime, timedelta
import os
import time
import pandas as pd
import yfinance as yf
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_crypto_data(symbol, directory="/root/sugihkan/crypto"):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        file_path = f"{directory}/{symbol}_data.csv"
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
    except Exception as e:
        logging.error(f"Error updating market data for {symbol}: {e}")
    return file_path

cryptos = [
    "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD",
    "BNB-USD", "XRP-USD", "USDT-USD", "DOGE-USD",
    "LTC-USD", "MATIC-USD", "LINK-USD", "FLOW-USD",
    "THETA-USD", "API3-USD", "MANA-USD", "MTL-USD",
    "ICP-USD", "ETH-BTC"
]

while True:
    for crypto in cryptos:
        update_crypto_data(crypto)
    time.sleep(60)  # Wait for one minute before the next update
