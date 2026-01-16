# Mock enrichment logic as requested in notes.md
import random

class Enricher:
    @staticmethod
    def mock_geocode(address):
        """
        Mock geocoding: returns random lat/lon.
        """
        return random.uniform(-90, 90), random.uniform(-180, 180)

    @staticmethod
    def mock_h3_index(lat, lon, resolution=9):
        """
        Mock H3 indexing: returns a dummy hex string.
        """
        # H3 indices are usually 15-character hex strings
        return hex(random.getrandbits(60))[2:].zfill(15)

    @classmethod
    def enrich_dataframe(self, df):
        """
        Adds mock lat, lon, and H3 index to the dataframe.
        """
        if 'address' in df.columns:
            coords = df['address'].apply(lambda x: self.mock_geocode(x))
            df['latitude'] = coords.apply(lambda x: x[0])
            df['longitude'] = coords.apply(lambda x: x[1])
            df['h3_index'] = df.apply(lambda row: self.mock_h3_index(row['latitude'], row['longitude']), axis=1)
        return df

    @staticmethod
    def enrich_spark(df):
        """
        Spark-native enrichment using Pandas UDFs for efficient distributed execution.
        """
        try:
            from pyspark.sql.functions import pandas_udf
            from pyspark.sql.types import DoubleType, StringType
        except ImportError:
            raise ImportError("PySpark not installed")

        # Define UDFs that wrapping the existing logic
        # Note: In a real app we'd vectorizing this properly, but for mock parity we wrap the item-level func
        
        import pandas as pd
        
        @pandas_udf(DoubleType())
        def get_lat(address_series: pd.Series) -> pd.Series:
            # Reusing mock_geocode logic locally
            # In distributed context, this runs on workers
            import random
            def _mock_lat(addr):
                 # Deterministic mock based on hash or just random
                 return random.uniform(-90, 90)
            return address_series.apply(_mock_lat)

        @pandas_udf(DoubleType())
        def get_lon(address_series: pd.Series) -> pd.Series:
            import random
            def _mock_lon(addr):
                return random.uniform(-180, 180)
            return address_series.apply(_mock_lon)

        @pandas_udf(StringType())
        def get_h3(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
            import random
            # Just return a mock string
            return lat_series.apply(lambda x: hex(random.getrandbits(60))[2:].zfill(15))

        if 'address' in df.columns:
            df = df.withColumn("latitude", get_lat(df['address']))
            df = df.withColumn("longitude", get_lon(df['address']))
            df = df.withColumn("h3_index", get_h3(df['latitude'], df['longitude']))
            
        return df
