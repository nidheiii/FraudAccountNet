import pandas as pd
import random
from datetime import datetime, timedelta
import os

# ---------------------------------------
# CONFIG
# ---------------------------------------
NUM_ACCOUNTS = 300
NUM_TRANSACTIONS = 8000
MULE_PERCENT = 0.15

DATA_DIR = "data_files"
os.makedirs(DATA_DIR, exist_ok=True)

random.seed(42)

# ---------------------------------------
# ACCOUNTS
# ---------------------------------------
cities = ["Chennai", "Bangalore", "Mumbai", "Delhi"]
countries = ["India", "UK", "USA"]

accounts = []
mule_accounts = []

for i in range(NUM_ACCOUNTS):
    acc_id = f"ACC{i:04d}"
    is_mule = 1 if random.random() < MULE_PERCENT else 0

    if is_mule:
        mule_accounts.append(acc_id)

    income = random.randint(200000, 1500000)

    accounts.append({
        "account_id": acc_id,
        "identity_verified": random.choice([0, 1]),
        "pan_verified": random.choice([0, 1]),
        "aadhar_verified": random.choice([0, 1]),
        "declared_income": income,
        "city": random.choice(cities),
        "country": random.choice(countries),
        "account_type": random.choice(["savings", "current"]),
        "is_mule": is_mule
    })

accounts_df = pd.DataFrame(accounts)
accounts_df.to_csv(f"{DATA_DIR}/indian_accounts.csv", index=False)

account_list = accounts_df["account_id"].tolist()

# ---------------------------------------
# HELPERS
# ---------------------------------------
def get_country(acc):
    return accounts_df.loc[
        accounts_df["account_id"] == acc, "country"
    ].values[0]

def random_time():
    return datetime.now() - timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

# ---------------------------------------
# TRANSACTIONS
# ---------------------------------------
transactions = []
tx_id = 0

def add_tx(sender, receiver, amount, ts, fraud, login_country=None):
    global tx_id

    transactions.append({
        "transaction_id": f"TX{tx_id:06d}",
        "from_account": sender,
        "to_account": receiver,
        "amount": float(amount),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "channel": random.choice(["UPI", "IMPS", "CARD"]),
        "is_fraud": fraud,
        "sender_country": get_country(sender),
        "receiver_country": get_country(receiver),
        "login_country": login_country if login_country else get_country(sender),
        "registered_country": get_country(sender)
    })

    tx_id += 1


# ---------------------------------------
# NORMAL BEHAVIOR
# ---------------------------------------
for _ in range(int(NUM_TRANSACTIONS * 0.6)):
    s, r = random.sample(account_list, 2)
    add_tx(s, r, random.choice([500, 2000, 5000, 12000, 25000]), random_time(), 0)


# ---------------------------------------
# 1. Dormant → sudden spike
# ---------------------------------------
for acc in mule_accounts[:15]:
    old = datetime.now() - timedelta(days=200)
    add_tx(acc, random.choice(account_list), 500, old, 0)

    for _ in range(5):
        add_tx(acc, random.choice(account_list), 60000, datetime.now(), 1)


# ---------------------------------------
# 2. Rapid withdrawal after deposit
# ---------------------------------------
for _ in range(300):
    a, b = random.sample(account_list, 2)
    ts = random_time()

    add_tx(a, b, 50000, ts, 1)
    add_tx(b, random.choice(account_list), 50000, ts + timedelta(minutes=1), 1)


# ---------------------------------------
# 3. Smurfing (₹49,999)
# ---------------------------------------
for acc in mule_accounts[:20]:
    for _ in range(5):
        add_tx(acc, random.choice(account_list), 49999, random_time(), 1)


# ---------------------------------------
# 4. Chain laundering (IMPORTANT)
# ---------------------------------------
for _ in range(150):
    chain = random.sample(mule_accounts, min(4, len(mule_accounts)))
    base = random.choice([50000, 100000])

    ts = random_time()
    for i in range(len(chain) - 1):
        add_tx(chain[i], chain[i + 1], base, ts + timedelta(minutes=i), 1)


# ---------------------------------------
# 5. Round amounts
# ---------------------------------------
for _ in range(300):
    s, r = random.sample(account_list, 2)
    add_tx(s, r, random.choice([10000, 50000, 100000]), random_time(), 1)


# ---------------------------------------
# 6. Cross-border anomaly
# ---------------------------------------
for _ in range(300):
    s = random.choice(account_list)
    r = random.choice(account_list)

    while get_country(s) == get_country(r):
        r = random.choice(account_list)

    add_tx(s, r, 70000, random_time(), 1)


# ---------------------------------------
# 7. Repetitive timed transactions
# ---------------------------------------
for acc in mule_accounts[:10]:
    base = random_time()
    for i in range(6):
        add_tx(acc, random.choice(account_list), 20000,
               base + timedelta(minutes=10*i), 1)


# ---------------------------------------
# 8. KYC inconsistent (amount > income)
# ---------------------------------------
for acc in mule_accounts[:15]:
    income = accounts_df.loc[
        accounts_df["account_id"] == acc, "declared_income"
    ].values[0]

    add_tx(acc, random.choice(account_list), income * 2, random_time(), 1)


# ---------------------------------------
# 9. Geo mismatch (login != registered)
# ---------------------------------------
for _ in range(300):
    s, r = random.sample(account_list, 2)

    fake_country = random.choice(
        [c for c in countries if c != get_country(s)]
    )

    add_tx(s, r, 20000, random_time(), 1, login_country=fake_country)


# ---------------------------------------
# 10. Limited counterparties
# ---------------------------------------
for acc in mule_accounts[:10]:
    partners = random.sample(account_list, 2)

    for _ in range(10):
        add_tx(acc, random.choice(partners), 30000, random_time(), 1)


# ---------------------------------------
# SAVE TRANSACTIONS (SAFE WRITE)
# ---------------------------------------
tx_path = f"{DATA_DIR}/indian_transactions.csv"
if os.path.exists(tx_path):
    os.remove(tx_path)

transactions_df = pd.DataFrame(transactions)
transactions_df.to_csv(tx_path, index=False)


# ---------------------------------------
# DEVICES
# ---------------------------------------
devices = []
for acc in account_list:
    devices.append({
        "device_id": f"DEV{random.randint(1,150)}",
        "account_id": acc,
        "device_type": random.choice(["Android", "iOS"]),
        "os": random.choice(["Android", "iOS"])
    })

pd.DataFrame(devices).to_csv(f"{DATA_DIR}/indian_devices.csv", index=False)

print("Realistic fraud dataset generated successfully.")
print(f"Accounts: {NUM_ACCOUNTS}")
print(f"Transactions: {len(transactions_df)}")
print(f"Mule accounts: {len(mule_accounts)}")