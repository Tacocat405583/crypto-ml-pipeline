import json
import requests
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
ATTEMPTS = 3

file_path = Path(__file__).parent / "ticker_price.jsonl"

class CoinbaseClient:

    def __init__(self,url):
        self._url = url


    def get_spot_price(self) -> dict:
        data = self._fetch()

        if data == False:
            return {}

        try:
            price = float(data['data']['amount'])
        except (KeyError, TypeError, ValueError) as e:
            print(f"bad payload: {type(e).__name__}: {e}", file=sys.stderr)
            return {}

        if price <= 0:
            print(f"Implausible price: {price}", file=sys.stderr)
            return {}

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "price": price,
        }

    def append_json(self,data:dict,path:Path=file_path):

        with open(path,"a",encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")

    def _fetch(self):

        for i in range(ATTEMPTS):

            try:
                #give time to get response and pass data
                response = requests.get(self._url,timeout=(5,10))

                if response.status_code == 200:
                    data = response.json()
                    return data
            except requests.RequestException as e:
                print(f"Error: {e}",file=sys.stderr)
                if i < ATTEMPTS - 1: # one less sleep
                    time.sleep(10)
                continue

        return False


if __name__ == "__main__":
    client = CoinbaseClient(url)
    record = client.get_spot_price()
    if not record:
        sys.exit(1)
    else:
        client.append_json(record)
    print(record)
