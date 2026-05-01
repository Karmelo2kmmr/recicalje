"""
ON-CHAIN REDEEM SCRIPT - Polymarket CTF Exchange
Redeems all winning positions directly via the Polygon blockchain.
"""
import os
import json
import time
import requests
from web3 import Web3
from eth_account import Account
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
# Try multiple public RPCs for Polygon
RPC_URLS = [
    "https://rpc-mainnet.matic.quiknode.pro",
    "https://matic-mainnet.chainstacklabs.com",
    "https://rpc-mainnet.maticvigil.com",
    "https://polygon.llamarpc.com",
    "https://endpoints.omniatech.io/v1/matic/mainnet/public",
    "https://1rpc.io/matic",
]

w3 = None
for rpc in RPC_URLS:
    try:
        candidate = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        if candidate.is_connected():
            chain = candidate.eth.chain_id
            if chain == 137:
                w3 = candidate
                print(f"Connected via: {rpc}")
                break
    except Exception:
        continue

if w3 is None:
    print("ERROR: Could not connect to any Polygon RPC")
    exit(1)
PRIVATE_KEY = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip()
if PRIVATE_KEY.startswith("0x"):
    PRIVATE_KEY = PRIVATE_KEY[2:]
FUNDER = Web3.to_checksum_address(os.environ.get("POLYMARKET_FUNDER", "").strip())
ADDRESS = Web3.to_checksum_address(os.environ.get("POLYMARKET_ADDRESS", "").strip())

# Polymarket Conditional Token Framework (CTF) contract on Polygon
CTF_ADDRESS = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

# Minimal ABI for redeeming positions
CTF_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "outputs": []
    }
]

USDC_POLYGON = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
ZERO_BYTES32 = b'\x00' * 32

account = Account.from_key(PRIVATE_KEY)
ctf = w3.eth.contract(address=CTF_ADDRESS, abi=CTF_ABI)

print(f"=== ON-CHAIN REDEEM ===")
print(f"Signer: {account.address}")
print(f"Network: {w3.eth.chain_id}")
print(f"Block: {w3.eth.block_number}")

# Fetch redeemable positions from Polymarket data API
positions_resp = requests.get(
    f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0",
    timeout=15
)
all_positions = positions_resp.json()
redeemable = [p for p in all_positions if p.get("redeemable")]
print(f"\nRedeemable positions found: {len(redeemable)}")

if not redeemable:
    print("No redeemable positions found. Nothing to do.")
    exit(0)

nonce = w3.eth.get_transaction_count(account.address)

for pos in redeemable:
    condition_id_hex = pos.get("conditionId", "")
    size = pos.get("size", 0)
    market = pos.get("market", "")[:60]
    outcome = pos.get("outcome", "")
    
    print(f"\n--- Redeeming ---")
    print(f"  Market: {market}")
    print(f"  Outcome: {outcome} | Size: {size}")
    print(f"  ConditionId: {condition_id_hex[:30]}...")
    
    # conditionId needs to be bytes32
    if condition_id_hex.startswith("0x"):
        condition_id_bytes = bytes.fromhex(condition_id_hex[2:])
    else:
        condition_id_bytes = bytes.fromhex(condition_id_hex)
    
    # Pad to 32 bytes
    condition_id_bytes = condition_id_bytes.ljust(32, b'\x00')[:32]
    
    # indexSets: [1] = outcome 0 (No/Down), [2] = outcome 1 (Yes/Up), [3] = both
    # We try indexSet [1, 2] to cover both outcomes
    for index_set in [[1], [2]]:
        try:
            tx = ctf.functions.redeemPositions(
                USDC_POLYGON,
                ZERO_BYTES32,
                condition_id_bytes,
                index_set
            ).build_transaction({
                "from": account.address,
                "nonce": nonce,
                "gas": 300000,
                "gasPrice": w3.to_wei("50", "gwei"),
                "chainId": 137,
            })
            
            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"  Sent tx (indexSet={index_set}): 0x{tx_hash.hex()}")
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            print(f"  Status: {'SUCCESS' if receipt.status == 1 else 'FAILED'} | Gas used: {receipt.gasUsed}")
            nonce += 1
            time.sleep(2)
        except Exception as e:
            err = str(e)
            if "execution reverted" in err or "revert" in err.lower():
                print(f"  indexSet={index_set}: No winning shares (expected) - {err[:80]}")
            else:
                print(f"  indexSet={index_set}: ERROR - {err[:120]}")

print("\n=== DONE ===")
print("Check your Polymarket portfolio for updated balance.")
