import pandas as pd
from graph.neo4j_loader import Neo4jConnection


# =========================================================
# MODULE 4A — CORE TRANSACTION GRAPH
# =========================================================
def build_transaction_graph(tx_csv_path, neo4j_conn: Neo4jConnection):
    tx = pd.read_csv(tx_csv_path)

    # -----------------------------
    # 1️⃣ Create Account Nodes
    # -----------------------------
    neo4j_conn.query("""
        UNWIND $accounts AS acc
        MERGE (:Account {id: acc})
    """, {
        "accounts": pd.concat([
            tx["from_account"],
            tx["to_account"]
        ]).unique().tolist()
    })

    # -----------------------------
    # 2️⃣ Create SENT Relationships
    # -----------------------------
    neo4j_conn.query("""
        UNWIND $rows AS row
        MATCH (a:Account {id: row.from_account})
        MATCH (b:Account {id: row.to_account})
        MERGE (a)-[:SENT {
            amount: row.amount,
            risk: row.txn_risk,
            patterns: row.patterns
        }]->(b)
    """, {
        "rows": [
            {
                "from_account": r.from_account,
                "to_account": r.to_account,
                "amount": float(r.amount),
                "txn_risk": float(r.TXN_RISK_SCORE),
                "patterns": (
                    ",".join(eval(r.DETECTED_PATTERNS))
                    if isinstance(r.DETECTED_PATTERNS, str)
                    else ""
                )
            }
            for r in tx.itertuples()
        ]
    })


# =========================================================
# MODULE 4B — LOCATION EXTENSION
# =========================================================
def add_location_nodes(tx_csv_path, neo4j_conn: Neo4jConnection):
    tx = pd.read_csv(tx_csv_path)

    # -----------------------------
    # 3️⃣ Create Location Nodes
    # -----------------------------
    neo4j_conn.query("""
        UNWIND $rows AS row
        MERGE (l:Location {country: row.country})
    """, {
        "rows": (
            tx[["login_country"]]
            .dropna()
            .rename(columns={"login_country": "country"})
            .to_dict("records")
        )
    })

    # -----------------------------
    # 4️⃣ Link Account → Location
    # -----------------------------
    neo4j_conn.query("""
        UNWIND $rows AS row
        MATCH (a:Account {id: row.account})
        MATCH (l:Location {country: row.country})
        MERGE (a)-[:ACCESSED_FROM]->(l)
    """, {
        "rows": (
            tx[["from_account", "login_country"]]
            .dropna()
            .rename(columns={
                "from_account": "account",
                "login_country": "country"
            })
            .to_dict("records")
        )
    })


# =========================================================
# MODULE 4C — DEVICE EXTENSION
# =========================================================
def add_device_nodes(tx_csv_path, neo4j_conn: Neo4jConnection):
    tx = pd.read_csv(tx_csv_path)

    # -----------------------------
    # 5️⃣ Create Device Nodes
    # -----------------------------
    devices = [
        {"device_id": f"DEV_{acc}"}
        for acc in tx["from_account"].unique()
    ]

    neo4j_conn.query("""
        UNWIND $rows AS row
        MERGE (d:Device {id: row.device_id})
    """, {"rows": devices})

    # -----------------------------
    # 6️⃣ Link Account → Device
    # -----------------------------
    links = [
        {"account": acc, "device": f"DEV_{acc}"}
        for acc in tx["from_account"].unique()
    ]

    neo4j_conn.query("""
        UNWIND $rows AS row
        MATCH (a:Account {id: row.account})
        MATCH (d:Device {id: row.device})
        MERGE (a)-[:LOGGED_IN_FROM]->(d)
    """, {"rows": links})
