import logging
import os

def get_logger():
    # Get absolute path to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, "outputs")

    # Ensure outputs/ exists
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "fraud.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        force=True  # 🔴 IMPORTANT for Python 3.13+
    )

    return logging.getLogger("FraudAccountNet")
