import pandas as pd
import numpy as np

DORMANT_DAYS = 180
RAPID_SECONDS = 15 * 60
SMURF_MIN = 45000
SMURF_MAX = 50000
ROUND_AMOUNTS = {10000, 25000, 50000, 100000}
REPEAT_COUNT = 5

def detect_transaction_patterns(tx: pd.DataFrame, accounts: pd.DataFrame = None):

    tx = tx.sort_values("timestamp").reset_index(drop=True)

    # -------------------------------
    # TIME FEATURES
    # -------------------------------
    tx["hour"] = tx["timestamp"].dt.hour
    tx["is_night"] = tx["hour"].isin([23, 0, 1, 2, 3, 4, 5]).astype(int)

    tx["prev_time"] = tx.groupby("from_account")["timestamp"].shift(1)

    tx["seconds_since_last_txn"] = (
        tx["timestamp"] - tx["prev_time"]
    ).dt.total_seconds().fillna(999999)

    tx["days_since_last_txn"] = tx["seconds_since_last_txn"] / 86400

    tx["repeat_amount_count"] = (
        tx.groupby(["from_account", "amount"])["transaction_id"]
        .transform("count")
    )

    tx["unique_counterparties"] = (
        tx.groupby("from_account")["to_account"]
        .transform("nunique")
    )

    # -------------------------------
    # PATTERNS
    # -------------------------------
    tx["DORMANT_ACCOUNT_ACTIVATION"] = (tx["days_since_last_txn"] >= DORMANT_DAYS).astype(int)

    tx["SUDDEN_DEPOSIT_WITHDRAWAL"] = (
        (tx["seconds_since_last_txn"] <= RAPID_SECONDS) &
        (tx["amount"] >= 50000)
    ).astype(int)

    tx["SMURFING"] = (
        (tx["amount"] >= SMURF_MIN) &
        (tx["amount"] < SMURF_MAX)
    ).astype(int)

    tx["RAPID_FUND_MOVEMENT"] = (
        tx["seconds_since_last_txn"] <= RAPID_SECONDS
    ).astype(int)

    tx["ROUND_NUMBER_TXN"] = (
        tx["amount"].isin(ROUND_AMOUNTS)
    ).astype(int)

    # ✅ NEW: NIGHT PATTERN
    tx["NIGHT_TXN"] = tx["is_night"]

    # -------------------------------
    # GEO / CROSS BORDER
    # -------------------------------
    tx["CROSS_BORDER"] = 0
    tx["GEO_ANOMALY"] = 0

    if "sender_country" in tx.columns:
        tx["CROSS_BORDER"] = (
            tx["sender_country"] != tx["receiver_country"]
        ).astype(int)

    if "login_country" in tx.columns:
        tx["GEO_ANOMALY"] = (
            tx["login_country"] != tx["registered_country"]
        ).astype(int)

    # -------------------------------
    # RECURRENT
    # -------------------------------
    tx["RECURRENT_PATTERN"] = (
        tx["repeat_amount_count"] >= REPEAT_COUNT
    ).astype(int)

    # -------------------------------
    # KYC
    # -------------------------------
    tx["KYC_MISMATCH"] = 0
    if accounts is not None:
        tx = tx.merge(
            accounts[["account_id", "declared_income"]],
            left_on="from_account",
            right_on="account_id",
            how="left"
        )

        tx["KYC_MISMATCH"] = (
            tx["amount"] > (tx["declared_income"] * 0.5)
        ).astype(int)

    # -------------------------------
    # PATTERN LIST
    # -------------------------------
    pattern_cols = [
        "DORMANT_ACCOUNT_ACTIVATION",
        "SUDDEN_DEPOSIT_WITHDRAWAL",
        "SMURFING",
        "RAPID_FUND_MOVEMENT",
        "ROUND_NUMBER_TXN",
        "NIGHT_TXN",
        "CROSS_BORDER",
        "RECURRENT_PATTERN",
        "KYC_MISMATCH",
        "GEO_ANOMALY"
    ]

    def collect_patterns(row):
        return [p for p in pattern_cols if row[p] == 1]

    tx["DETECTED_PATTERNS"] = tx.apply(collect_patterns, axis=1)

    # -------------------------------
    # 🔥 IMPROVED RISK SCORING
    # -------------------------------
    weights = {
        "DORMANT_ACCOUNT_ACTIVATION": 0.3,
        "SUDDEN_DEPOSIT_WITHDRAWAL": 0.3,
        "SMURFING": 0.25,
        "RAPID_FUND_MOVEMENT": 0.2,
        "ROUND_NUMBER_TXN": 0.15,
        "NIGHT_TXN": 0.15,
        "CROSS_BORDER": 0.25,
        "RECURRENT_PATTERN": 0.2,
        "KYC_MISMATCH": 0.3,
        "GEO_ANOMALY": 0.25
    }

    def compute_score(patterns):
        score = sum(weights[p] for p in patterns)
        return min(score, 1.0)

    tx["TXN_RISK_SCORE"] = tx["DETECTED_PATTERNS"].apply(compute_score)

    return tx