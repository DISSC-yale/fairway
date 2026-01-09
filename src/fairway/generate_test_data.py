import pandas as pd
import numpy as np
import os

def generate_test_data(size="small", partitioned=True, file_format="csv"):
    os.makedirs('data/raw', exist_ok=True)
    
    if size == "small":
        n_rows = 100
        file_path = f'data/raw/sales.{file_format}'
        output_path = f'data/raw/sales_partitioned'
    else:
        n_rows = 1000000
        file_path = f'data/raw/sales_large.{file_format}'
        output_path = f'data/raw/sales_large_partitioned'
        
    df = pd.DataFrame({
        'id': range(n_rows),
        'date': pd.date_range('2023-01-01', periods=n_rows, freq='min' if size=="large" else 'D'),
        'amount': np.random.uniform(10, 1000, n_rows),
        'address': [f"{i} Street Name, City" for i in range(n_rows)]
    })
    
    if partitioned:
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        
        if file_format == 'parquet':
            df.to_parquet(output_path, partition_cols=['year', 'month'], index=False)
            print(f"Generated {size} partitioned parquet dataset at {output_path}")
        else:
            # Manual partitioning for CSV
            for (year, month), group in df.groupby(['year', 'month']):
                partition_dir = os.path.join(output_path, f"year={year}", f"month={month}")
                os.makedirs(partition_dir, exist_ok=True)
                # Using a simple filename strategy, could be more robust
                partition_file = os.path.join(partition_dir, f"sales_part_{year}_{month}.csv")
                group.to_csv(partition_file, index=False)
            print(f"Generated {size} partitioned csv dataset at {output_path}")

    else:
        if file_format == 'parquet':
             # Adjust extension if needed, though file_path usually has extension logic above
             # treating file_path base as the target
             file_path = file_path.replace('.csv', '.parquet') 
             df.to_parquet(file_path, index=False)
        else:
             df.to_csv(file_path, index=False)
        print(f"Generated {size} {file_format} dataset at {file_path}")

if __name__ == "__main__":
    generate_test_data("small", partitioned=True)
    # Uncomment for large data test
    # generate_test_data("large", partitioned=True)
