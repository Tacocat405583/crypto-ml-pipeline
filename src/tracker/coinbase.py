import requests


url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

response = requests.get(url)

print(response.status_code)
print(response.text)

data = response.json()
print(data)





