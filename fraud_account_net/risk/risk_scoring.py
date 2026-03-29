def compute_risk(row):
    score = 0.0

    if row["total_volume"] > 5_000_000:
        score += 0.3
    elif row["total_volume"] > 1_000_000:
        score += 0.2

    if row["night_activity"] > 0.2:
        score += 0.2

    if row["fraud_count"] > 0:
        score += 0.3

    score += row["avg_txn_risk"] * 0.2
    return min(score, 1.0)
