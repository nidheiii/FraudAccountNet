def format_inr(amount):
    if amount >= 1e7:
        return f"₹{amount/1e7:.1f}Cr"
    elif amount >= 1e5:
        return f"₹{amount/1e5:.1f}L"
    elif amount >= 1e3:
        return f"₹{amount/1e3:.1f}K"
    return f"₹{amount:.0f}"
