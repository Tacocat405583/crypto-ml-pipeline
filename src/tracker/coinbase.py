import json
import requests
from datetime import datetime, timezone
from pathlib import Path
import sys

url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"


file_path = Path(__file__).parent / "ticker_price.jsonl"

class CoinbaseClient:

    def __init__(self,url):
        self._url = url


    def get_spot_price(self) -> dict:
        data = self._fetch()

        if data == False:
            return {}


        price = float(data['data']['amount'])
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "price": price,
        }

    def append_json(self,data:dict,path:Path=file_path):

        with open(path,"a",encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")

    def _fetch(self):

        try:
            response = requests.get(self._url,timeout=(5,10))

            if response.status_code == 200:
                data = response.json()
                return data
            else:
                return False
        except requests.exceptions.ConnectTimeout:
            print("The connection timed out.")
        except requests.exceptions.ReadTimeout:
            print("The server took too long to send data.")
        except requests.exceptions.Timeout:
            print("The request timed out generally.")
        except requests.RequestException as e:
            print(f"Error: {e}",file=sys.stderr)




if __name__ == "__main__":
    client = CoinbaseClient(url)
    record = client.get_spot_price()
    if not record:
        sys.exit(1)
    else:
        client.append_json(record)
    print(record)
