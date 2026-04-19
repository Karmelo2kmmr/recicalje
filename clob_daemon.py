import os
import sys
import json
import time
import socket
import logging
from threading import Thread
import math

try:
    from dotenv import load_dotenv
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, MarketOrderArgs, OrderType, BalanceAllowanceParams, AssetType
except ImportError:
    print(json.dumps({"status": "error", "message": "Missing dependencies."}))
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] daemon: %(message)s")

client = None

def init_client():
    global client
    load_dotenv()
    proxy_url = os.environ.get("PROXY_URL", "").strip()
    if proxy_url:
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["ALL_PROXY"] = proxy_url

    raw_pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip() or os.environ.get("WALLET_PRIVATE_KEY", "").strip()
    if raw_pk.startswith("0x"): raw_pk = raw_pk[2:]
    
    api_key = os.environ.get("POLYMARKET_API_KEY", "").strip()
    api_secret = os.environ.get("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE", "").strip()
    funder = os.environ.get("POLYMARKET_FUNDER", "").strip()

    if not raw_pk or not api_key:
        logging.error("Missing Polymarket credentials in .env")
        return False

    try:
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        client = ClobClient(
            host=os.environ.get("POLYMARKET_HOST", "https://clob.polymarket.com").strip() or "https://clob.polymarket.com",
            key=raw_pk,
            chain_id=137,
            creds=creds,
            signature_type=2,
            funder=funder if funder else None,
        )
        logging.info("Polymarket CLOB Client Initialized.")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize client: {str(e)}")
        return False

def handle_buy(req):
    try:
        token_id = req["token_id"]
        usdc_size = float(req["usdc_size"])
        limit_price = round(float(req["limit_price"]), 4)
        shares = math.floor((usdc_size / limit_price) * 1_000_000) / 1_000_000.0

        order_args = OrderArgs(token_id=token_id, price=limit_price, size=shares, side="BUY")
        signed_order = client.create_order(order_args)
        resp = client.post_order(signed_order, OrderType.GTC)

        if resp.get("success") is False or resp.get("status") == "error":
            return {"status": "error", "message": f"API Rejected: {resp}"}

        order_id = resp.get("orderID", resp.get("id", "unknown"))
        return {"status": "ok", "order_id": order_id, "shares_ordered": shares, "limit_price": limit_price}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_sell(req):
    try:
        token_id = req["token_id"]
        token_qty = math.floor(float(req["token_qty"]) * 1_000_000) / 1_000_000.0
        limit_price = round(float(req["limit_price"]), 4)
        order_type_str = req.get("order_type", "GTC")

        # Check balance first
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        res = client.get_balance_allowance(params)
        actual_balance = float(res.get("balance", "0")) / 1_000_000.0

        if actual_balance == 0:
             return {"status": "error", "message": "Zero on-chain balance."}

        token_qty = min(token_qty, actual_balance)

        if order_type_str == "FAK":
            order_args = MarketOrderArgs(token_id=token_id, amount=token_qty, price=limit_price, side="SELL", order_type=OrderType.FAK)
            signed_order = client.create_market_order(order_args)
            resp = client.post_order(signed_order, OrderType.FAK)
        else:
            order_args = OrderArgs(token_id=token_id, size=token_qty, price=limit_price, side="SELL")
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order, OrderType.GTC)

        if resp.get("success") is False or resp.get("status") == "error":
            return {"status": "error", "message": str(resp)}

        order_id = resp.get("orderID", resp.get("id", "unknown"))
        return {"status": "ok", "order_id": order_id, "shares_sold": token_qty}
    except Exception as e:
         return {"status": "error", "message": str(e)}

def handle_cancel(req):
    try:
        resp = client.cancel(req["order_id"])
        return {"status": "ok", "cancelled_order_id": req["order_id"], "raw": resp}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_balance(req):
    try:
        token_id = req["token_id"]
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        res = client.get_balance_allowance(params)
        actual_balance = float(res.get("balance", "0")) / 1_000_000.0
        return {"status": "ok", "actual_balance": actual_balance}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def process_client(conn):
    with conn:
        try:
            data = conn.recv(4096)
            if not data: return
            req = json.loads(data.decode("utf-8"))
            cmd = req.get("cmd")

            resp = {"status": "error", "message": f"Unknown cmd {cmd}"}
            if cmd == "buy": resp = handle_buy(req)
            elif cmd == "sell": resp = handle_sell(req)
            elif cmd == "cancel": resp = handle_cancel(req)
            elif cmd == "reconcile_balance": resp = handle_balance(req)
            elif cmd == "ping": resp = {"status": "ok"}

            conn.sendall(json.dumps(resp).encode("utf-8") + b"\n")
        except Exception as e:
            err = {"status": "error", "message": f"Daemon decode error: {e}"}
            conn.sendall(json.dumps(err).encode("utf-8") + b"\n")

def start_server():
    if not init_client():
        return
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 50051))
    s.listen(10)
    logging.info("CLOB Python Daemon listening on 127.0.0.1:50051")
    while True:
        conn, addr = s.accept()
        Thread(target=process_client, args=(conn,)).start()

if __name__ == "__main__":
    start_server()
