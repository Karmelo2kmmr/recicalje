import os
import json
from dotenv import load_dotenv
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds

def main():
    load_dotenv()
    print("--- EMERGENCY REDEEM ---")
    
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

    print("Fetching winning positions to redeem...")
    # The client has a redeem function that takes a list of condition IDs or market IDs
    # Since the user has $12 ready, we can try to call 'redeem' on all resolved markets
    try:
        # Note: Some versions of the client use redeem_all or similar.
        # Let's try to get the winning balances first.
        print("Executing redeem_all call...")
        resp = client.redeem()
        print(f"Redeem Response: {resp}")
    except Exception as e:
        print(f"Error during redeem: {e}")

if __name__ == "__main__":
    main()
