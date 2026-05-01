import requests
import json
import sys

# Fetch positions from Polymarket data API
FUNDER = "0x726e90de4808ff19BC25dc64CB057822432A1Cd1"
ADDRESS = "0x855bccbB05691ddfDfac6571aB09F8B7C7A43Ad6"

print("--- POSITION SCAN ---")
redeemable_positions = []

for addr in [ADDRESS, FUNDER]:
    url = f"https://data-api.polymarket.com/positions?user={addr}&sizeThreshold=0"
    try:
        resp = requests.get(url, timeout=15)
        data = resp.json()
        print(f"\nWallet: {addr} | Total positions: {len(data)}")
        for p in data[:30]:
            market = str(p.get("market", ""))[:60]
            outcome = p.get("outcome", "")
            size = p.get("size", 0)
            cur_price = p.get("curPrice", 0)
            redeemable = p.get("redeemable", False)
            condition_id = p.get("conditionId", "")
            token_id = p.get("asset", "")
            value = float(size) * float(cur_price) if size and cur_price else 0
            tag = "REDEEMABLE" if redeemable else "active"
            print(f"  [{tag}] {market}")
            print(f"           outcome={outcome} | size={size} | curPrice={cur_price} | value=${value:.2f}")
            print(f"           conditionId={condition_id[:30]} | tokenId={token_id[:30]}")
            if redeemable:
                redeemable_positions.append({
                    "addr": addr,
                    "market": market,
                    "outcome": outcome,
                    "size": size,
                    "value": value,
                    "condition_id": condition_id,
                    "token_id": token_id
                })
    except Exception as e:
        print(f"Error for {addr}: {e}", file=sys.stderr)

print(f"\n\nSUMMARY: Found {len(redeemable_positions)} redeemable positions")
total_value = sum(p["value"] for p in redeemable_positions)
print(f"Total redeemable value: ${total_value:.2f}")

# Save to json for redeem script
with open("redeemable.json", "w") as f:
    json.dump(redeemable_positions, f, indent=2)
print("Saved to redeemable.json")
