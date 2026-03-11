import csv
import json
import urllib.request
import os
from datetime import datetime, timedelta, timezone
import sys

# Asegurar salida UTF-8 en Windows
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# --- CONFIGURACIÓN ---
# New York es UTC-4 (EDT)
def get_daily_csv(yesterday=False):
    now_et = datetime.now(timezone(timedelta(hours=-4)))
    if yesterday:
        now_et = now_et - timedelta(days=1)
    return f"trades_{now_et.strftime('%Y-%m-%d')}.csv"

# Manejo de argumentos
is_yesterday = "--yesterday" in sys.argv
file_arg = None
if "--file" in sys.argv:
    idx = sys.argv.index("--file")
    if idx + 1 < len(sys.argv):
        file_arg = sys.argv[idx + 1]

if file_arg:
    CSV_FILE = file_arg
else:
    dated_file = get_daily_csv(yesterday=is_yesterday)
    if os.path.exists(dated_file):
        CSV_FILE = dated_file
    elif os.path.exists("trades.csv"):
        CSV_FILE = "trades.csv"
    else:
        CSV_FILE = dated_file 

GAMMA_URL = "https://gamma-api.polymarket.com/markets/"

# Colores ANSI
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"

def fetch_market_outcome(market_id):
    if not market_id or market_id.strip() == "":
        return "N/A"
    try:
        url = f"{GAMMA_URL}{market_id.strip()}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0"
        })
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            if data.get("umaResolutionStatus") != "resolved":
                return "PENDING"
            
            prices = data.get("outcomePrices")
            if isinstance(prices, str):
                try: prices = json.loads(prices)
                except: pass
            
            if not prices or len(prices) < 2:
                return "RESOLVING"
            
            p0 = float(prices[0])
            p1 = float(prices[1])

            if p0 > 0.9: return "UP"
            elif p1 > 0.9: return "DOWN"
            return "RESOLVING"
    except Exception as e:
        return f"ERR"

def audit_trade(side, status, outcome, entry_price=0.0, exit_price=0.0):
    if outcome == "PENDING":
        return f"{YELLOW}PENDING{RESET}"
    if outcome == "N/A":
        return "N/A"
    
    is_loss = "LOSS" in status.upper()
    is_win = "WIN" in status.upper()
    
    # SL/TP detection
    if is_loss and exit_price <= 0.68 and exit_price > 0.1:
        return f"{GREEN}VERIFIED (SL){RESET}"
    if is_win and exit_price >= 0.95:
        return f"{GREEN}VERIFIED (TP){RESET}"

    expected_win = (side.upper() == outcome)
    if is_win == expected_win:
        return f"{GREEN}VERIFIED{RESET}"
    else:
        if is_loss and expected_win:
             return f"{YELLOW}SCALPED/CUT{RESET}"
        return f"{BOLD}{RED}⚠️ DISCREPANCY{RESET}"

def format_row(row):
    status = row.get("STATUS", "").strip()
    if "WIN" in status:
        row["STATUS"] = f"{GREEN}{status}{RESET}"
    elif "LOSS" in status:
        row["STATUS"] = f"{RED}{status}{RESET}"
    
    pnl = row.get("PNL", "").strip()
    if "+" in pnl:
        row["PNL"] = f"{GREEN}{pnl}{RESET}"
    elif "-" in pnl:
        row["PNL"] = f"{RED}{pnl}{RESET}"
    return row

def main():
    if not os.path.exists(CSV_FILE):
        print(f"{RED}Error: {CSV_FILE} no encontrado.{RESET}")
        return

    print(f"\n{BOLD}{CYAN}      BOT RECICLAJE - AUDITORÍA DE TRADES (CLOUD){RESET}")
    print(f"{CYAN}" + "—" * 175 + f"{RESET}")
    
    # Header format updated to current bot format
    # TIME (ET) | COIN | SIDE | ENTRY | EXIT | REZ | STATUS | PNL | RET% | STRAT | DCA | VOLAT | MARKET_ID | EQUITY_BEFORE | STAKE | EQUITY_AFTER | AUDIT
    headers = [
        "TIME (ET)", "COIN", "SIDE", "ENTRY", "EXIT", "REZ", "STATUS", 
        "PNL", "RET%", "STRAT", "DCA", "VOLAT", "MARKET_ID", "EQUITY_BEFORE", "STAKE", "EQUITY_AFTER", "AUDIT"
    ]
    
    header_fmt = "{:<10} | {:<5} | {:<4} | {:<6} | {:<6} | {:<3} | {:<22} | {:<10} | {:<7} | {:<15} | {:<3} | {:<8} | {:<12} | {:<8} | {:<8} | {:<8} | {}"
    print(header_fmt.format(*headers))
    print("—" * 185)

    total_pnl = 0.0
    wins = 0
    losses = 0
    total_trades = 0

    try:
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            lines = f.readlines()
            if not lines: return
            
            for line in lines:
                if "TIME" in line or not line.strip(): continue
                
                parts = [p.strip() for p in line.split('|')]
                if len(parts) < 13: continue
                
                row_data = {
                    "TIME": parts[0],
                    "COIN": parts[1],
                    "SIDE": parts[2],
                    "ENTRY": parts[3],
                    "EXIT": parts[4],
                    "REZ": parts[5],
                    "STATUS": parts[6],
                    "PNL": parts[7],
                    "RET%": parts[8],
                    "STRAT": parts[9],
                    "DCA": parts[10],
                    "VOLAT": parts[11],
                    "MARKET_ID": parts[12],
                    "EQUITY_BEFORE": parts[13] if len(parts) > 13 else "N/A",
                    "STAKE": parts[14] if len(parts) > 14 else "N/A",
                    "EQUITY_AFTER": parts[15] if len(parts) > 15 else "N/A",
                }

                try:
                    entry_p = float(row_data["ENTRY"])
                    exit_p = float(row_data["EXIT"])
                except:
                    entry_p = 0.0
                    exit_p = 0.0

                outcome = fetch_market_outcome(row_data["MARKET_ID"])
                audit_status = audit_trade(row_data["SIDE"], row_data["STATUS"], outcome, entry_p, exit_p)
                disp = format_row(row_data.copy())
                
                print(header_fmt.format(
                    disp["TIME"], disp["COIN"], disp["SIDE"], disp["ENTRY"], disp["EXIT"], 
                    disp["REZ"], disp["STATUS"], disp["PNL"], disp["RET%"], 
                    disp["STRAT"], disp["DCA"], disp["VOLAT"], disp["MARKET_ID"][:8]+"..", 
                    disp["EQUITY_BEFORE"], disp["STAKE"], disp["EQUITY_AFTER"], audit_status
                ))

                try:
                    pnl_raw = disp["PNL"].replace(GREEN, "").replace(RED, "").replace(RESET, "").strip()
                    if "$+" in pnl_raw: total_pnl += float(pnl_raw.split("$+")[1])
                    elif "$-" in pnl_raw: total_pnl -= float(pnl_raw.split("$-")[1])
                    
                    status_raw = disp["STATUS"].replace(GREEN, "").replace(RED, "").replace(RESET, "").upper()
                    if "WIN" in status_raw: wins += 1
                    elif "LOSS" in status_raw: losses += 1
                    total_trades += 1
                except: pass

            print(f"\n{BOLD}{CYAN}" + "—" * 100 + f"{RESET}")
            pnl_color = GREEN if total_pnl >= 0 else RED
            pnl_sign = "+" if total_pnl >= 0 else "-"
            print(f"{BOLD}💰 PNL REAL AUDITADO: {pnl_color}${pnl_sign}{abs(total_pnl):.2f}{RESET}")
            print(f"📊 Aciertos: {GREEN}{wins}{RESET} | Fallos: {RED}{losses}{RESET} | Total: {total_trades}")
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
            print(f"🎯 TASA DE ACIERTO: {BOLD}{YELLOW}{win_rate:.1f}%{RESET}")
            print(f"{CYAN}" + "—" * 100 + f"{RESET}\n")

    except Exception as e:
        print(f"{RED}Error: {e}{RESET}")

if __name__ == "__main__":
    main()
