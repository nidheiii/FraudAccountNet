def device_sharing(devices_df):
    return (
        devices_df.groupby("device_id")["account_id"]
        .nunique()
        .reset_index(name="accounts_per_device")
    )
