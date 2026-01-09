import os
import pandas as pd
from datetime import datetime

def setup_mock_complex_dataset(base_dir="data/raw/complex_stats"):
    os.makedirs(base_dir, exist_ok=True)
    
    states = ["CT", "NY", "NJ"]
    years = [2022, 2023, 2024]
    
    for state in states:
        for year in years:
            # Create a few snapshots per year
            for month in ["01", "06"]:
                fname = f"stats--{state}--{year}-{month}-01.csv"
                fpath = os.path.join(base_dir, fname)
                
                # Create dummy data
                df = pd.DataFrame({
                    "id": range(10),
                    "value": [i * 10 for i in range(10)],
                    "timestamp": [datetime.now().isoformat()] * 10
                })
                df.to_csv(fpath, index=False)
                print(f"Created {fpath}")

if __name__ == "__main__":
    setup_mock_complex_dataset()
