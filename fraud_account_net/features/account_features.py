def rolling_account_features(df):
    df = df.sort_values("timestamp")

    rolling = (
        df.groupby("from_account")
        .rolling("7D", on="timestamp")["amount"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )

    rolling.columns = [
        "from_account", "timestamp",
        "avg_amt_7d", "std_amt_7d", "tx_count_7d"
    ]

    return df.merge(
        rolling,
        on=["from_account", "timestamp"],
        how="left"
    ).fillna(0)
