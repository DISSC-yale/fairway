import pandas as pd
import os
from datetime import datetime

class Summarizer:
    @staticmethod
    def generate_summary_table(df, output_path):
        """
        Generates advanced summary statistics (min, max, mean, nulls, distinct).
        """
        stats = []
        for col in df.columns:
            column_data = df[col]
            type_name = str(column_data.dtype)
            
            col_stats = {
                "variable": col,
                "type": type_name,
                "null_count": int(column_data.isnull().sum()),
                "distinct_count": int(column_data.nunique()),
            }
            
            if pd.api.types.is_numeric_dtype(column_data):
                col_stats.update({
                    "min": column_data.min(),
                    "max": column_data.max(),
                    "mean": column_data.mean(),
                    "median": column_data.median()
                })
            
            stats.append(col_stats)
            
        summary_df = pd.DataFrame(stats)
        summary_df.to_csv(output_path, index=False)
        return summary_df

    @staticmethod
    def generate_summary_spark(df, output_path):
        """
        Generates summary statistics using Spark native functions.
        """
        # Spark's summary() provides count, mean, stddev, min, max, but not null/distinct
        # We can implement a robust one or just use .summary().toPandas() for now as it returns a tiny DF
        
        # 1. Basic Stats
        desc = df.describe().toPandas()
        
        # 2. Nulls and Distincts (expensive)
        # We can optimize this by doing one big agg if needed
        # For prototype, we'll skip distinct count on huge data if it's too slow, or use approx_count_distinct
        
        from pyspark.sql import functions as F
        
        aggs = []
        for col in df.columns:
            aggs.append(F.count(F.col(col)).alias(f"{col}_count"))
            aggs.append(F.count(F.when(F.col(col).isNull(), 1)).alias(f"{col}_nulls"))
            # aggs.append(F.approx_count_distinct(col).alias(f"{col}_distinct")) # Optional
            
        # Execute one pass
        # stats_row = df.agg(*aggs).collect()[0]
        
        # Reformatting to match the pandas structure requires transposing the describe() output
        # For minimal disruption, we transpose the describe() output
        
        # desc is:
        # summary | col1 | col2
        # count   | 10   | 10
        # mean    | ...  | ...
        
        desc = desc.set_index("summary").transpose().reset_index().rename(columns={"index": "variable"})
        
        # Just save it
        desc.to_csv(output_path, index=False)
        return desc

    @staticmethod
    def generate_markdown_report(dataset_name, summary_df, stats, output_path):
        """
        Generates a MkDocs-compatible Markdown report for the dataset.
        """
        lines = [
            f"# {dataset_name}",
            "\n## Overview",
            f"- **Processed at**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- **Total Rows**: {stats.get('row_count'):,}",
            f"- **Total Columns**: {len(summary_df)}",
            "\n## Variable Summary",
            summary_df.to_markdown(index=False),
            "\n## Lineage",
            f"- **Status**: {stats.get('status')}",
            f"- **Source Config**: `{stats.get('config_path', 'unknown')}`"
        ]
        
        with open(output_path, 'w') as f:
            f.write("\n".join(lines))
        print(f"Markdown report generated at {output_path}")
