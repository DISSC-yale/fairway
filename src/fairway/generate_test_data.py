import pandas as pd
import numpy as np
import os

def generate_test_data(size="small", partitioned=True):
    os.makedirs('data/raw', exist_ok=True)
    
    if size == "small":
        n_rows = 100
        file_path = 'data/raw/sales.csv'
        parquet_path = 'data/raw/sales_partitioned'
    else:
        n_rows = 1000000
        file_path = 'data/raw/sales_large.csv'
        parquet_path = 'data/raw/sales_large_partitioned'
        
    df = pd.DataFrame({
        'id': range(n_rows),
        'date': pd.date_range('2023-01-01', periods=n_rows, freq='min' if size=="large" else 'D'),
        'amount': np.random.uniform(10, 1000, n_rows),
        'address': [f"{i} Street Name, City" for i in range(n_rows)]
    })
    
    if partitioned:
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df.to_parquet(parquet_path, partition_cols=['year', 'month'], index=False)
        print(f"Generated {size} partitioned dataset at {parquet_path}")
    else:
        df.to_csv(file_path, index=False)
        print(f"Generated {size} dataset at {file_path}")

if __name__ == "__main__":
    generate_test_data("small", partitioned=True)
    # Uncomment for large data test
    # generate_test_data("large", partitioned=True)
