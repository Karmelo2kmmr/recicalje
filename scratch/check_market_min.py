import socket
import json

def call_daemon(cmd_req):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 50051))
    s.sendall(json.dumps(cmd_req).encode("utf-8"))
    resp = s.recv(65536)
    s.close()
    return json.loads(resp.decode("utf-8"))

def check_min_size(market_id):
    # We can't directly query one market metadata from current daemon 
    # but we can fetch all markets and find it.
    resp = call_daemon({"cmd": "get_markets"})
    if resp["status"] != "ok":
        print(f"Error fetching markets: {resp}")
        return

    for m in resp["data"]:
        if str(m.get("id")) == str(market_id) or str(m.get("conditionId")) == str(market_id):
            print(f"Market: {m.get('question')}")
            # The gamma api response structure usually has 'minimum_order_size' or similar
            # in some fields, but for CLOB it is often 5 USDC.
            print(json.dumps(m, indent=2))
            return

check_min_size("2113134")
