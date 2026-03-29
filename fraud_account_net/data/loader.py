import pandas as pd
import os
from datetime import datetime, timedelta
import random

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data_files")

TX_FILE = os.path.join(DATA_DIR, "indian_transactions.csv")
ACC_FILE = os.path.join(DATA_DIR, "indian_accounts.csv")
DEV_FILE = os.path.join(DATA_DIR, "indian_devices.csv")


def generate_demo_data():
    os.makedirs(DATA_DIR, exist_ok=True)

    # -------- Accounts --------
    accounts = []
    for i in range(1, 51):
        accounts.append({
            "account_id": f"ACC{i:03d}",
            "identity_verified": random.choice([0, 1]),
            "pan_verified": random.choice([0, 1]),
            "aadhar_verified": random.choice([0, 1]),
            "city": random.choice(["Chennai", "Bangalore", "Mumbai", "Delhi"]),
            "state": random.choice(["TN", "KA", "MH", "DL"]),
            "account_type": random.choice(["savings", "current"])
        })

    acc_df = pd.DataFrame(accounts)
    acc_df.to_csv(ACC_FILE, index=False)

    # -------- Devices --------
    devices = []
    for i in range(1, 31):
        devices.append({
            "device_id": f"DEV{i:03d}",
            "account_id": random.choice(acc_df["account_id"]),
            "device_type": random.choice(["Android", "iOS"]),
            "os": random.choice(["Android", "iOS"])
        })

    dev_df = pd.DataFrame(devices)
    dev_df.to_csv(DEV_FILE, index=False)

    # -------- Transactions --------
    transactions = []
    start = datetime.now() - timedelta(days=30)

    for i in range(1, 1001):
        amt = random.choice([500, 1200, 5000, 10000, 25000, 48000, 50000, 100000, 250000])
        ts = start + timedelta(minutes=random.randint(1, 40000))

        transactions.append({
            "transaction_id": f"TX{i:06d}",
            "from_account": random.choice(acc_df["account_id"]),
            "to_account": random.choice(acc_df["account_id"]),
            "amount": amt,
            "timestamp": ts,
            "channel": random.choice(["UPI", "IMPS", "CARD"]),
            "is_fraud": 1 if amt >= 48000 and random.random() < 0.15 else 0
        })

    tx_df = pd.DataFrame(transactions)
    tx_df.to_csv(TX_FILE, index=False)


def load_data():
    if not (os.path.exists(TX_FILE) and os.path.exists(ACC_FILE) and os.path.exists(DEV_FILE)):
        print("⚠️ Demo data not found. Generating sample Indian banking data...")
        generate_demo_data()

    tx = pd.read_csv(TX_FILE)
    acc = pd.read_csv(ACC_FILE)
    dev = pd.read_csv(DEV_FILE)

    tx["timestamp"] = pd.to_datetime(tx["timestamp"])
    return tx, acc, dev
