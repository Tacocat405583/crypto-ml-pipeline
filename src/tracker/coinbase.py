import json
import requests
from datetime import datetime, timezone
from pathlib import Path

url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"


file_path = Path(__file__).parent / "ticker_price.jsonl"

class CoinbaseClient:

    def __init__(self,url):
        self._url = url


    def get_spot_price(self) -> dict:

        response = requests.get(self._url)
        data = response.json()
        price = float(data['data']['amount'])

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "price": price,
        }

    def append_json(self,data:dict,path:Path=file_path):

        with open(path,"a",encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")



if __name__ == "__main__":
    client = CoinbaseClient(url)
    record = client.get_spot_price()
    client.append_json(record)
    print(record)
