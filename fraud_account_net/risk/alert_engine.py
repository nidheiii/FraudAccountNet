def assign_alert_priority(row):
    score = row["TXN_RISK_SCORE"]

    if score >= 0.7:
        return "CRITICAL"
    elif score >= 0.5:
        return "HIGH"
    elif score >= 0.3:
        return "MEDIUM"
    else:
        return "LOW"