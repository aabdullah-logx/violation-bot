import os
import json
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# --- Authentication ---
token = os.getenv("TOKEN")

# --- Configuration Flags ---
# Convert string "False" or "True" to actual Boolean types
LOCAL = os.getenv("LOCAL", "False").lower() == "true"
LOCAL_DB = os.getenv("LOCAL_DB", "False").lower() == "true"

# --- URL Constants ---
BASE_URL = os.getenv("BASE_URL", "https://sellercentral.amazon.com")
AMAZON_HOME = os.getenv("AMAZON_HOME")
A_Z_CLAIMS_PATH = os.getenv("A_Z_CLAIMS")

# --- Policy Violations Dictionary ---
# We parse the string from .env back into a Python dictionary
raw_paths = os.getenv("POLICY_VIOLATION_PATHS", "{}")
policy_violation_url_list = json.loads(raw_paths)

def get_full_url(path_key):
    """Utility to combine base URL with a specific policy path."""
    path = POLICY_VIOLATION_URLS.get(path_key)
    if path:
        return f"{BASE_URL}{path}"
    return None

# Example usage within this file for verification:
if __name__ == "__main__":
    print(f"Database Mode: {'Local' if LOCAL_DB else 'QuickBase'}")
    print(f"Sample Link: {get_full_url('intellectual_property_complaints')}")