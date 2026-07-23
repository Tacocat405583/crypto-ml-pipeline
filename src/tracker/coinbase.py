import requests
from pathlib import Path

url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"


file_path = Path("ticker_price.json")

class CoinbaseClient:

    def __init__(self,url):
        self._url = url


    def get_spot_price(self):

        response = requests.get(self._url)
        data = response.json()
        data = data['data']['amount']
        self.append_json(data=data)

        return data
    
    def append_json(self,data:dict):

        if file_path.exists():
            with open("ticker_price.json","a",encoding="utf-8") as f:
                f.write(str(data))
        else:
            with open("ticker_price.json","w",encoding="utf-8") as f:
                f.write(str(data))
            
    

if __name__ == "__main__":
    client = CoinbaseClient(url)
    print(client.get_spot_price())







