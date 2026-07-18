import requests


url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

# response = requests.get(url)

# print(response.status_code)
# print(response.text)

# data = response.json()

# print("This is after parsing")
# print(data)

class CoinbaseClient:

    def __init__(self,url):
        self._url = url


    def get_spot_price(self):

        response = requests.get(self._url)
        data = response.json()

        return data
    
    def append_json(self):

        
    

if __name__ == "__main__":
    client = CoinbaseClient(url)
    print(client.get_spot_price())







