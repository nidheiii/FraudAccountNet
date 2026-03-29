import pandas as pd

from data.loader import load_data
from data.validator import validate

from features.transaction_features import (
    build_transaction_features,
    add_transaction_context
)

from risk.transaction_pattern_engine import detect_transaction_patterns
from risk.risk_scoring import compute_risk
from risk.alert_engine import assign_alert_priority


# =========================================================
# MAIN PIPELINE
# =========================================================
def main():
    print("FraudAccountNet starting...\n")

    # -----------------------------------------------------
    # 1. LOAD DATA
    # -----------------------------------------------------
    tx, accounts, devices = load_data()
    print("Data loaded")
    print(f"Total transactions loaded: {len(tx)}\n")

    # -----------------------------------------------------
    # 2. VALIDATION
    # -----------------------------------------------------
    tx = validate(tx)
    print("Validation passed\n")

    # Normalize timestamp
    tx["timestamp"] = pd.to_datetime(tx["timestamp"])

    # -----------------------------------------------------
    # 3. TRANSACTION FEATURE ENGINEERING
    # -----------------------------------------------------
    tx = build_transaction_features(
        tx,
        night_hours=[23, 0, 1, 2, 3, 4, 5]
    )
    tx = add_transaction_context(tx)

    print("Transaction features added")
    print("Sample engineered transactions (before risk scoring):")
    print(
        tx[[
            "transaction_id",
            "amount",
            "is_night"
        ]].head(5),
        "\n"
    )

    # -----------------------------------------------------
    # 4. FRAUD PATTERN DETECTION
    #    (Creates TXN_RISK_SCORE)
    # -----------------------------------------------------
    tx = detect_transaction_patterns(tx, accounts)

    print("Fraud patterns detected")
    print("Sample transaction risk scores and patterns:")
    print(
        tx[[
            "transaction_id",
            "TXN_RISK_SCORE",
            "DETECTED_PATTERNS",
            "is_fraud"
        ]].head(10),
        "\n"
    )

    # -----------------------------------------------------
    # 5. ACCOUNT-LEVEL RISK AGGREGATION
    # -----------------------------------------------------
    account_df = (
        tx.groupby("from_account")
        .agg(
            tx_count=("transaction_id", "count"),
            fraud_count=("is_fraud", "sum"),
            total_volume=("amount", "sum"),
            avg_amount=("amount", "mean"),
            night_activity=("is_night", "mean"),
            fraud_ratio=("is_fraud", "mean"),
            avg_txn_risk=("TXN_RISK_SCORE", "mean"),
        )
        .reset_index()
    )

    account_df["account_risk_score"] = account_df.apply(
        compute_risk, axis=1
    )

    print("Account-level risk aggregation completed")
    print("Sample account risk profiles:")
    print(account_df.head(10), "\n")

    # -----------------------------------------------------
    # 6. ALERT PRIORITY ASSIGNMENT
    # -----------------------------------------------------
    tx["ALERT_PRIORITY"] = tx.apply(
        assign_alert_priority, axis=1
    )

    print("Alert priority assigned")
    print(
        tx[[
            "transaction_id",
            "TXN_RISK_SCORE",
            "ALERT_PRIORITY"
        ]].head(10),
        "\n"
    )

    # -----------------------------------------------------
    # 7. SAVE OUTPUTS
    # -----------------------------------------------------
    tx.to_csv(
        "outputs/transactions_with_all_patterns.csv",
        index=False
    )
    account_df.to_csv(
        "outputs/account_risk_scores.csv",
        index=False
    )

    print("Outputs saved successfully:")
    print(" - outputs/transactions_with_all_patterns.csv")
    print(" - outputs/account_risk_scores.csv\n")

    print("FraudAccountNet completed successfully.")


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    main()
