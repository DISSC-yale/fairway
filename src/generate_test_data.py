import pandas as pd
import numpy as np
import os

def generate_test_data(size="small"):
    os.makedirs('data/raw', exist_ok=True)
    
    if size == "small":
        n_rows = 100
        file_path = 'data/raw/sales.csv'
    else:
        n_rows = 1000000
        file_path = 'data/raw/sales_large.csv'
        
    df = pd.DataFrame({
        'id': range(n_rows),
        'date': pd.date_range('2023-01-01', periods=n_rows, freq='min' if size=="large" else 'D'),
        'amount': np.random.uniform(10, 1000, n_rows),
        'address': [f"{i} Street Name, City" for i in range(n_rows)]
    })
    
    df.to_csv(file_path, index=False)
    print(f"Generated {size} dataset at {file_path}")

if __name__ == "__main__":
    generate_test_data("small")
    # Uncomment for large data test
    # generate_test_data("large")
