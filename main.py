import requests
import sys
import decimal
import time
import os
from datetime import datetime
import csv
import asyncio
import websockets
import json



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

async def ws_get_orderbook():
    # Connect to the WebSocket server
    async with websockets.connect(f"wss://ws.api.prod.paradex.trade/v1") as websocket:
        
        # Send the subscription request
        request_message = {
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {
                "channel": "markets_summary"
            },
            "id": 1
        }
        await websocket.send(json.dumps(request_message))
        
        # Continuously listen for incoming messages
        while True:
            # Receive a message from the WebSocket server
            message = await websocket.recv()
            message = json.loads(message, parse_float=decimal.Decimal)
            data = message.get('params', {}).get('data', None)
            
            if data == None:
                continue
            
            symbol = data.get('symbol')
            if symbol.endswith('-P') == False and symbol.endswith('-C') == False:
                continue
            
            # Print the received message
            result = data.copy()  
            greeks = result.pop('greeks')  
            result.update(greeks)
            persist_result(symbol, result)

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
        asyncio.get_event_loop().run_until_complete(ws_get_orderbook())

