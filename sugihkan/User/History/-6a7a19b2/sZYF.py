from datetime import datetime, timedelta
import os
import time
import pandas as pd
import yfinance as yf

def update_crypto_data(crypto_pair):
    # Set the directory to /root/sugihkan/crypto
    directory = "/root/sugihkan/crypto"
    
    # Try to create the directory and catch any errors
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except Exception as e:
        print(f"Error creating directory: {e}")
        return None  # Stop the function if the directory can't be created

    # Set the file path for the CSV
    file_path = f"{directory}/{crypto_pair.replace('/', '')}_data.csv"
    
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
        new_data = yf.download(crypto_pair, start=start_date, end=end_date, interval='1m')
        if not new_data.empty:
            new_data.reset_index(inplace=True)
            new_data.rename(columns={'Datetime': 'index'}, inplace=True)
            updated_data = pd.concat([existing_data, new_data])
            updated_data.to_csv(file_path, index=False)

    return file_path

cryptos = [
    "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD",
    "SOL-USD", "ADA-USD",
    "MATIC-USD", "SUI-USD", "LINK-USD", "FLOW-USD",
    "WLD-USD", "CTK-USD", "BIGTIME-USD", "THETA-USD",
    "API3-USD", "MANA-USDT", "MTL-USD", "ARB-USD",
    "BOND-USD", "ICP-USDT", "ETH-BTC"
]

while True:
    for crypto in cryptos:
        file_created = update_crypto_data(crypto)
        if file_created:
            print(f"Updated data for {crypto}")
        else:
            print(f"Failed to update data for {crypto}")
    time.sleep(60)  # Wait for one minute before next update