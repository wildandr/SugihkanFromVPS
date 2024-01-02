import os
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def get_datetime_column(dataframe):
    # Identify the column which contains datetime information
    for col in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(dataframe[col]):
            return col
    return None

def update_crypto_data(crypto_pair):
    directory = "/root/sugihkan/crypto"
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except Exception as e:
        print(f"Error creating directory: {e}")
        return None

    file_path = f"{directory}/{crypto_pair.replace('/', '')}_data.csv"
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
        if not existing_data.empty:
            datetime_column = get_datetime_column(existing_data)
            if datetime_column is None:
                print("No datetime column found in existing data.")
                return None
            last_date_str = existing_data[datetime_column].max()
            last_date = pd.to_datetime(last_date_str)
            last_date = last_date.replace(tzinfo=None)
            if last_date > start_date:
                start_date = last_date + timedelta(minutes=1)
    else:
        existing_data = pd.DataFrame()

    if start_date < end_date:
        new_data = yf.download(crypto_pair, start=start_date, end=end_date, interval='1m')
        if not new_data.empty:
            new_data.reset_index(inplace=True)
            new_data.rename(columns={'index': 'Datetime'}, inplace=True)
            updated_data = pd.concat([existing_data, new_data])
            updated_data.to_csv(file_path, index=False)

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
        file_created = update_crypto_data(crypto)
        if file_created:
            print(f"Updated data for {crypto}")
        else:
            print(f"Failed to update data for {crypto}")
    time.sleep(60)
