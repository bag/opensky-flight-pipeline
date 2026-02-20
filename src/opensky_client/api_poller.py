import requests
import json
from datetime import datetime
from pathlib import Path

def fetch_and_write(output_path: str):
    """Fetch OpenSky states and write to landing zone as JSON"""
    resp = requests.get("https://opensky-network.org/api/states/all")
    resp.raise_for_status()
    data = resp.json()
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{output_path}/opensky_{timestamp}.json"
    
    # For ADLS/Blob, use dbutils.fs.put or cloud SDK
    # For local testing:
    with open(filename, "w") as f:
        json.dump(data, f)
    
    return filename, len(data.get("states", []))

if __name__ == "__main__":
    path, count = fetch_and_write("/tmp/opensky_landing")
    print(f"Wrote {count} records to {path}")
