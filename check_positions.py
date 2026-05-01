import os
import json
from dotenv import load_dotenv
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
import requests

def main():
    load_dotenv()
    print("--- POSITION AUDIT ---")
    
    raw_pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip() or os.environ.get("WALLET_PRIVATE_KEY", "").strip()
    if raw_pk.startswith("0x"): raw_pk = raw_pk[2:]
    creds = ApiCreds(api_key=os.environ.get("POLYMARKET_API_KEY", "").strip(), 
                     api_secret=os.environ.get("POLYMARKET_API_SECRET", "").strip(), 
                     api_passphrase=os.environ.get("POLYMARKET_API_PASSPHRASE", "").strip())
    
    client = ClobClient(
        host=os.environ.get("POLYMARKET_HOST", "https://clob.polymarket.com").strip(),
        chain_id=137,
        key=raw_pk,
        creds=creds,
        signature_type=2,
        funder=os.environ.get("POLYMARKET_FUNDER", "").strip()
    )

    # Fetch active crypto markets
    url = "https://gamma-api.polymarket.com/markets?closed=false&limit=100&tag_id=1001"
    resp = requests.get(url)
    markets = resp.json()
    
    found = False
    for m in markets:
        for tid in [m.get("clobTokenIds")]:
            if not tid: continue
            ids = json.loads(tid)
            for token_id in ids:
                params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                res = client.get_balance_allowance(params)
                bal = float(res.get("balance", "0")) / 1_000_000.0
                if bal > 0.01:
                    print(f"FOUND POSITION: {m.get('question')} | Token: {token_id} | Balance: {bal}")
                    found = True
    
    if not found:
        print("No active token positions found.")

if __name__ == "__main__":
    main()
