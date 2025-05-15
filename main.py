import requests
import sys
import decimal
import time
import os
from datetime import datetime
import csv




RESULTS_DIR = '/home/alvaro/culebra_research_results'

def get_orderbook(market_symbol: str):

    headers = {
        'Accept': 'application/json',
    }

    try:
        greeks_response = requests.get('https://api.prod.paradex.trade/v1/markets/summary', params={'market':market_symbol}, headers=headers)
        greeks_response.raise_for_status() 

        greeks_data = greeks_response.json()['results'][0]
        result = greeks_data.copy()  
        greeks = result.pop('greeks')  
        result.update(greeks)  

        return result
    except requests.RequestException as e:
        print(f"Error fetching order book: {e}")
    except KeyError:
        print("Unexpected response structure. Please check the market symbol or API changes.")

def persist_result(market_symbol, result):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    csv_filename = os.path.join(RESULTS_DIR, f'{market_symbol}_{current_date}.csv')
    file_exists = os.path.exists(csv_filename)
    with open(csv_filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=result.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(result)

if __name__ == "__main__":
    while True:
        for market_symbol in ["SOL-USD-170-C", "SOL-USD-170-P", "SOL-USD-175-C", "SOL-USD-175-P", "SOL-USD-180-C", "SOL-USD-180-P"]:
            result = get_orderbook(market_symbol)
            persist_result(market_symbol, result)
        
        time.sleep(60)

