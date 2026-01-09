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
