import numpy as np

def build_transaction_features(df, night_hours):
    """
    Basic temporal and behavioral transaction features
    """
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_night"] = df["hour"].isin(night_hours).astype(int)
    df["amount_log"] = np.log1p(df["amount"])
    return df


def add_transaction_context(df):
    """
    Contextual transaction features for pattern detection
    """
    df = df.sort_values("timestamp")

    df["prev_txn_time"] = df.groupby("from_account")["timestamp"].shift(1)

    df["seconds_since_last_txn"] = (
        df["timestamp"] - df["prev_txn_time"]
    ).dt.total_seconds().fillna(999999)

    df["days_since_last_txn"] = df["seconds_since_last_txn"] / 86400

    df["repeat_amount_count"] = (
        df.groupby(["from_account", "amount"])["transaction_id"]
        .transform("count")
    )

    df["unique_counterparties"] = (
        df.groupby("from_account")["to_account"]
        .transform("nunique")
    )

    return df
