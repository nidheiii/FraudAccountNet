import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check


schema = DataFrameSchema(
    {
        "transaction_id": Column(str),
        "from_account": Column(str),
        "to_account": Column(str),
        "amount": Column(float, Check.ge(0)),
        "timestamp": Column(pa.DateTime),
        "channel": Column(str),
        "is_fraud": Column(int, Check.isin([0, 1])),

        # ✅ ADD THESE (OPTIONAL BUT VALID)
        "sender_country": Column(str, nullable=True),
        "receiver_country": Column(str, nullable=True),
        "login_country": Column(str, nullable=True),
        "registered_country": Column(str, nullable=True),
    },
    strict=True
)

def validate(df):
    return schema.validate(df)
