import os
import json
from dotenv import load_dotenv
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds, AssetType

def main():
    load_dotenv()
    print("--- EMERGENCY CLEANUP ---")
    
    raw_pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip() or os.environ.get("WALLET_PRIVATE_KEY", "").strip()
    if raw_pk.startswith("0x"): raw_pk = raw_pk[2:]
    
    api_key = os.environ.get("POLYMARKET_API_KEY", "").strip()
    api_secret = os.environ.get("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE", "").strip()
    funder = os.environ.get("POLYMARKET_FUNDER", "").strip()
    
    creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = ClobClient(
        host=os.environ.get("POLYMARKET_HOST", "https://clob.polymarket.com").strip(),
        chain_id=137,
        key=raw_pk,
        creds=creds,
        signature_type=2,
        funder=funder
    )
    
    print("Fetching open orders...")
    resp = client.get_open_orders()
    orders = resp.get("data", []) if isinstance(resp, dict) else resp
    print(f"Found {len(orders)} open orders.")
    
    if len(orders) > 0:
        print("Cancelling all orders...")
        for o in orders:
            try:
                client.cancel_order(o.get("order_id") or o.get("id"))
                print(f"Cancelled: {o.get('order_id')}")
            except Exception as e:
                print(f"Error cancelling: {e}")
    
    print("Fetching real collateral balance...")
    from py_clob_client_v2.clob_types import BalanceAllowanceParams
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    res = client.get_balance_allowance(params)
    balance = float(res.get("balance", "0")) / 1_000_000.0
    print(f"USDC Available: ${balance}")
    
    if float(balance) < 5.0:
        print("WARNING: Collateral still below $5.00 position size.")
    else:
        print("SUCCESS: Capital liberated.")

if __name__ == "__main__":
    main()
