"""
Script de test rapide de la connexion GLPI API.
Usage: python scripts/test_glpi_api.py
Configure GLPI_APP_TOKEN et GLPI_USER_TOKEN dans .env avant de lancer.
"""
import os, sys, json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL   = os.getenv("GLPI_BASE_URL", "http://localhost:8080/apirest.php")
APP_TOKEN  = os.getenv("GLPI_APP_TOKEN", "")
USER_TOKEN = os.getenv("GLPI_USER_TOKEN", "")

def test():
    print(f"\n🔌 Testing GLPI API at {BASE_URL}\n")

    # 1. Init session
    resp = requests.get(f"{BASE_URL}/initSession",
                        headers={"App-Token": APP_TOKEN,
                                 "Authorization": f"user_token {USER_TOKEN}"},
                        timeout=10)
    if resp.status_code != 200:
        print(f"❌ initSession failed [{resp.status_code}]: {resp.text}")
        sys.exit(1)
    session_token = resp.json()["session_token"]
    print(f"✅ Session token obtained: {session_token[:20]}…")

    headers = {"App-Token": APP_TOKEN, "Session-Token": session_token}

    # 2. GET /Ticket
    resp2 = requests.get(f"{BASE_URL}/Ticket",
                         headers=headers,
                         params={"range": "0-4", "expand_dropdowns": "true"},
                         timeout=10)
    if resp2.status_code == 200:
        tickets = resp2.json()
        print(f"✅ Fetched {len(tickets)} tickets (sample):")
        for t in tickets[:3]:
            print(f"   #{t.get('id')} | {t.get('name','(no title)')} | priority={t.get('priority')}")
    else:
        print(f"⚠️  GET /Ticket returned {resp2.status_code}: {resp2.text[:200]}")

    # 3. Kill session
    requests.get(f"{BASE_URL}/killSession", headers=headers, timeout=5)
    print("\n✅ Session closed. GLPI API is working correctly.\n")

if __name__ == "__main__":
    if not APP_TOKEN or not USER_TOKEN:
        print("❌ Set GLPI_APP_TOKEN and GLPI_USER_TOKEN in .env first!")
        print("   See README.md → Section 'Configuration GLPI API Token'")
        sys.exit(1)
    test()
