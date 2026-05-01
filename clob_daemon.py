import os
import sys
import json
import time
import socket
import logging
import urllib.request
from threading import Thread
import math
from decimal import Decimal, ROUND_DOWN

try:
    from dotenv import load_dotenv
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import (
        ApiCreds,
        OrderArgs,
        MarketOrderArgs,
        OrderType,
        BalanceAllowanceParams,
        AssetType,
    )
except ImportError:
    print(json.dumps({"status": "error", "message": "Missing dependencies. Install py-clob-client-v2."}))
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] daemon: %(message)s")

client = None

def get_actual_onchain_balance(token_id):
    """External truth: what does the chain/API say right now?"""
    try:
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        res = client.get_balance_allowance(params)
        return float(res.get("balance", "0")) / 1_000_000.0
    except:
        return None

def get_confirmed_open_orders(token_id=None):
    """External truth: what orders are actually live on the book?"""
    try:
        # Note: In a real scenario, you might want to filter by token_id if the SDK supports it efficiently
        # Here we get all open orders and filter manually for safety
        orders = client.get_open_orders()
        if token_id:
            return [o for o in orders if o.get("token_id") == token_id]
        return orders
    except:
        return []

def build_reconciliation_response(order_id, accepted, filled_size, token_id):
    """Unified response format for P0 hardening."""
    balance_after = get_actual_onchain_balance(token_id)
    open_orders_after = get_confirmed_open_orders(token_id)
    
    # Calculate remaining size from the order perspective if possible
    # This is a bit tricky without the specific order object, so we mark it based on live list
    is_live = any(o.get("orderID") == order_id or o.get("id") == order_id for o in open_orders_after)
    
    return {
        "status": "ok" if accepted else "error",
        "order_id": order_id,
        "accepted": accepted,
        "filled_size": filled_size,
        "shares_sold": filled_size,
        "remaining_live": is_live,
        "confirmed_live_orders_count": len(open_orders_after),
        "confirmed_balance_after": balance_after,
    }

def parse_order_filled_size(order_details):
    """Extract matched/fill size from the field names returned by Polymarket."""
    if not isinstance(order_details, dict):
        return 0.0

    for key in ("size_matched", "size_filled", "filled_size", "matched_size"):
        raw = order_details.get(key)
        if raw not in (None, ""):
            try:
                return float(raw)
            except (TypeError, ValueError):
                pass

    nested = order_details.get("order")
    if isinstance(nested, dict):
        return parse_order_filled_size(nested)

    return 0.0

def normalize_market_buy_amount(usdc_size, limit_price):
    """Return a cents amount whose resulting shares fit CLOB precision limits."""
    price = Decimal(str(limit_price))
    if price <= 0:
        return round(usdc_size, 2)

    cents = int(Decimal(str(usdc_size)) * Decimal("100"))
    for raw_cents in range(cents, 0, -1):
        amount = Decimal(raw_cents) / Decimal("100")
        shares = amount / price
        if shares == shares.quantize(Decimal("0.0001"), rounding=ROUND_DOWN):
            return float(amount)

    return float((Decimal(cents) / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_DOWN))

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
    signature_type_raw = os.environ.get("POLYMARKET_SIGNATURE_TYPE", "").strip()

    if signature_type_raw:
        try:
            signature_type = int(signature_type_raw)
        except ValueError:
            logging.error("POLYMARKET_SIGNATURE_TYPE must be 0, 1, or 2; got %r", signature_type_raw)
            return False
    else:
        signature_type = 2 if funder else 0

    if signature_type not in (0, 1, 2):
        logging.error("POLYMARKET_SIGNATURE_TYPE must be 0, 1, or 2; got %s", signature_type)
        return False

    if signature_type in (1, 2) and not funder:
        logging.error(
            "POLYMARKET_FUNDER is required when POLYMARKET_SIGNATURE_TYPE=%s. "
            "Use the Polymarket profile wallet/proxy address as funder.",
            signature_type,
        )
        return False

    if not raw_pk or not api_key:
        logging.error("Missing Polymarket credentials in .env")
        return False

    try:
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        client = ClobClient(
            host=os.environ.get("POLYMARKET_HOST", "https://clob.polymarket.com").strip() or "https://clob.polymarket.com",
            chain_id=137,
            key=raw_pk,
            creds=creds,
            signature_type=signature_type,
            funder=funder if funder else None,
            retry_on_error=True,
        )
        logging.info(
            "Polymarket CLOB V2 Client Initialized. address=%s signature_type=%s funder=%s",
            client.get_address(),
            signature_type,
            funder if funder else client.get_address(),
        )
        return True
    except Exception as e:
        logging.error(f"Failed to initialize client: {str(e)}")
        return False

def handle_buy(req):
    try:
        token_id = req["token_id"]
        usdc_size = float(req["usdc_size"])
        limit_price = round(float(req["limit_price"]), 4)
        order_amount = normalize_market_buy_amount(usdc_size, limit_price)

        before_balance = get_actual_onchain_balance(token_id)
        if before_balance is None:
            return {"status": "error", "message": "Could not confirm pre-trade balance."}

        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=order_amount,
            price=limit_price,
            side="BUY",
            order_type=OrderType.FAK,
        )
        resp = client.create_and_post_market_order(order_args, order_type=OrderType.FAK)

        if resp.get("success") is False or resp.get("status") == "error":
            return {"status": "error", "message": f"API Rejected: {resp}"}

        # Wait for fill confirmation
        time.sleep(1.5)
        after_balance = get_actual_onchain_balance(token_id)
        order_id = resp.get("orderID", resp.get("id", "unknown"))
        filled_size = max(0.0, after_balance - before_balance) if after_balance is not None else 0.0
        
        # SECONDARY CHECK: If balance didn't move but we have an order_id, check the order status directly
        if filled_size <= 0 and order_id != "unknown":
            try:
                order_details = client.get_order(order_id)
                filled_size = parse_order_filled_size(order_details)
                logging.info(
                    "Balance check failed but OrderID %s confirms fill: %s status=%s",
                    order_id,
                    filled_size,
                    order_details.get("status"),
                )
            except Exception as e:
                logging.warn(f"Secondary order check failed for {order_id}: {e}")

        return build_reconciliation_response(order_id, True, filled_size, token_id)
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_health(_req):
    host = os.environ.get("POLYMARKET_HOST", "https://clob.polymarket.com").strip() or "https://clob.polymarket.com"
    url = f"{host.rstrip('/')}/version"
    try:
        started = time.perf_counter()
        request = urllib.request.Request(url, headers={"User-Agent": "arbitrage-hammer-health"})
        with urllib.request.urlopen(request, timeout=2.0) as response:
            body = response.read(256).decode("utf-8", errors="replace").lower()
            latency_ms = int((time.perf_counter() - started) * 1000)
            ready = response.status == 200 and not any(
                marker in body for marker in ("syncing", "not_ready", "not ready", "service not ready")
            )
            return {
                "status": "ok",
                "data": {
                    "ready": ready,
                    "http_status": response.status,
                    "latency_ms": latency_ms,
                    "body": body[:120],
                },
            }
    except Exception as e:
        return {
            "status": "ok",
            "message": f"health_check_failed: {e}",
            "data": {"ready": False, "reason": str(e)},
        }

def handle_sell(req):
    try:
        token_id = req["token_id"]
        token_qty = math.floor(float(req["token_qty"]) * 1_000_000) / 1_000_000.0
        limit_price = round(float(req["limit_price"]), 4)
        order_type_str = req.get("order_type", "GTC")

        actual_balance = get_actual_onchain_balance(token_id)
        if actual_balance is None or actual_balance == 0:
             return {"status": "error", "message": "Zero or unconfirmed on-chain balance."}

        token_qty = min(token_qty, actual_balance)

        if order_type_str == "FAK":
            order_args = MarketOrderArgs(token_id=token_id, amount=token_qty, price=limit_price, side="SELL", order_type=OrderType.FAK)
            resp = client.create_and_post_market_order(order_args, order_type=OrderType.FAK)
        else:
            order_args = OrderArgs(token_id=token_id, size=token_qty, price=limit_price, side="SELL")
            resp = client.create_and_post_order(order_args, order_type=OrderType.GTC)

        if resp.get("success") is False or resp.get("status") == "error":
            return {"status": "error", "message": str(resp)}

        # External reconciliation
        time.sleep(1.5)
        after_balance = get_actual_onchain_balance(token_id)
        shares_sold = max(0.0, actual_balance - after_balance) if after_balance is not None else 0.0
        
        order_id = resp.get("orderID", resp.get("id", "unknown"))

        # SECONDARY CHECK: If balance didn't move, check the order status directly
        if shares_sold <= 0 and order_id != "unknown":
            try:
                order_details = client.get_order(order_id)
                shares_sold = parse_order_filled_size(order_details)
                logging.info(
                    "Balance check failed but OrderID %s confirms sell fill: %s status=%s",
                    order_id,
                    shares_sold,
                    order_details.get("status"),
                )
            except Exception as e:
                logging.warn(f"Secondary order check failed for {order_id}: {e}")

        return build_reconciliation_response(order_id, True, shares_sold, token_id)
    except Exception as e:
         return {"status": "error", "message": str(e)}

def handle_cancel(req):
    try:
        order_id = req["order_id"]
        resp = client.cancel(order_id)
        
        # External HARD verification
        time.sleep(1.0)
        open_orders = get_confirmed_open_orders()
        still_live = any(o.get("orderID") == order_id or o.get("id") == order_id for o in open_orders)
        
        if still_live:
             return {"status": "error", "message": "Cancel requested but order remains live in external truth check."}
             
        return {"status": "ok", "cancelled_order_id": order_id, "confirmed_gone": True}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_get_order_status(req):
    try:
        order_id = req["order_id"]
        order_data = client.get_order(order_id)
        return {"status": "ok", "order": order_data}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_open_orders(req):
    try:
        token_id = req.get("token_id")
        orders = get_confirmed_open_orders(token_id)
        return {"status": "ok", "orders": orders}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_balance(req):
    try:
        token_id = req["token_id"]
        actual_balance = get_actual_onchain_balance(token_id)
        return {"status": "ok", "actual_balance": actual_balance}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_collateral_status(_req):
    try:
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        res = client.get_balance_allowance(params)
        balance = float(res.get("balance", "0")) / 1_000_000.0
        return {
            "status": "ok",
            "collateral_balance": balance,
            "raw": res,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

import requests

def handle_get_markets(req):
    try:
        url = "https://gamma-api.polymarket.com/markets"
        params = {"closed": "false", "limit": 1000, "order": "volume24hr", "ascending": "false"}
        if req.get("tag_id"): params["tag_id"] = req["tag_id"]
        
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        return {"status": "ok", "data": resp.json()}
    except Exception as e:
        return {"status": "error", "message": f"Python market fetch error: {str(e)}"}

def process_client(conn):
    with conn:
        try:
            data = conn.recv(65536)
            if not data: return
            req = json.loads(data.decode("utf-8"))
            cmd = req.get("cmd")

            resp = {"status": "error", "message": f"Unknown cmd {cmd}"}
            if cmd == "buy": resp = handle_buy(req)
            elif cmd == "sell": resp = handle_sell(req)
            elif cmd == "cancel": resp = handle_cancel(req)
            elif cmd == "get_order_status": resp = handle_get_order_status(req)
            elif cmd == "get_open_orders": resp = handle_open_orders(req)
            elif cmd == "reconcile_balance": resp = handle_balance(req)
            elif cmd == "collateral_status": resp = handle_collateral_status(req)
            elif cmd == "get_markets": resp = handle_get_markets(req)
            elif cmd == "health": resp = handle_health(req)
            elif cmd == "ping": resp = {"status": "ok"}

            res_json = json.dumps(resp).encode("utf-8") + b"\n"
            conn.sendall(res_json)
        except Exception as e:
            err = {"status": "error", "message": f"Daemon decode error: {e}"}
            conn.sendall(json.dumps(err).encode("utf-8") + b"\n")

def start_server():
    if not init_client(): return
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 50051))
    s.listen(10)
    logging.info("HARDENED CLOB Python Daemon listening on 127.0.0.1:50051")
    while True:
        conn, addr = s.accept()
        Thread(target=process_client, args=(conn,)).start()

if __name__ == "__main__":
    start_server()
