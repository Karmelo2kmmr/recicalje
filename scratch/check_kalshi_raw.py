import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

email = os.getenv("KALSHI_EMAIL")
password = os.getenv("KALSHI_PASSWORD")

# Login
resp = requests.post("https://api.elections.kalshi.com/trade-api/v2/login", json={"email": email, "password": password})
token = resp.json().get("token")

headers = {"Authorization": f"Bearer {token}"}

# Fetch KXBTC15M markets
resp = requests.get("https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXBTC15M&limit=1", headers=headers)
markets = resp.json().get("markets", [])

if markets:
    print(json.dumps(markets[0], indent=2))
else:
    print("No markets found")
