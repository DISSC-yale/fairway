"""PLACEHOLDER geospatial enrichment.

This module ships DETERMINISTIC MOCK values — not real geocoding. The mock
is stable across runs (same address → same lat/lon → same h3 index) so
downstream joins and tests don't flap, but the numbers are not meaningful
geographically. A real geocoder must replace these functions before the
enrichment can be used for analysis.

Pipeline code must opt in via ``enrichment.allow_mock: true`` in config;
otherwise ``IngestionPipeline`` refuses to run with geocode enrichment on,
preventing mock data from silently reaching production tables.
"""
import hashlib


def _hash_uniform(seed: bytes, lo: float, hi: float) -> float:
    """Deterministic value in [lo, hi) derived from ``seed`` via sha256."""
    digest = hashlib.sha256(seed).digest()
    frac = int.from_bytes(digest[:8], "big") / 2 ** 64
    return lo + frac * (hi - lo)


def _mock_lat_for(address) -> float:
    return _hash_uniform(f"lat|{address}".encode("utf-8"), -90.0, 90.0)


def _mock_lon_for(address) -> float:
    return _hash_uniform(f"lon|{address}".encode("utf-8"), -180.0, 180.0)


def _mock_h3_for(lat: float, lon: float, resolution: int = 9) -> str:
    seed = f"h3|{lat:.10f}|{lon:.10f}|{resolution}".encode("utf-8")
    return hashlib.sha256(seed).hexdigest()[:15]


class Enricher:
    @staticmethod
    def mock_geocode(address):
        """Return a deterministic (lat, lon) derived from ``address``."""
        return _mock_lat_for(address), _mock_lon_for(address)

    @staticmethod
    def mock_h3_index(lat, lon, resolution=9):
        """Return a deterministic 15-char hex mock H3 index derived from both lat and lon."""
        return _mock_h3_for(lat, lon, resolution)

    @staticmethod
    def enrich_dataframe(df):
        """Add mock lat, lon, and H3 index to the dataframe."""
        if 'address' in df.columns:
            coords = df['address'].apply(Enricher.mock_geocode)
            df['latitude'] = coords.apply(lambda x: x[0])
            df['longitude'] = coords.apply(lambda x: x[1])
            df['h3_index'] = df.apply(
                lambda row: Enricher.mock_h3_index(row['latitude'], row['longitude']),
                axis=1,
            )
        return df

    @staticmethod
    def enrich_spark(df):
        """Spark-native enrichment using pandas UDFs.

        UDFs call into the same deterministic helpers used by the pandas path
        so Spark and DuckDB produce identical mock values for the same input.
        """
        try:
            from pyspark.sql.functions import pandas_udf
            from pyspark.sql.types import DoubleType, StringType
        except ImportError:
            raise ImportError("PySpark not installed")

        import pandas as pd

        @pandas_udf(DoubleType())
        def get_lat(address_series: pd.Series) -> pd.Series:
            return address_series.apply(_mock_lat_for)

        @pandas_udf(DoubleType())
        def get_lon(address_series: pd.Series) -> pd.Series:
            return address_series.apply(_mock_lon_for)

        @pandas_udf(StringType())
        def get_h3(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
            return pd.Series(
                [_mock_h3_for(lat, lon) for lat, lon in zip(lat_series, lon_series)],
                index=lat_series.index,
            )

        if 'address' in df.columns:
            df = df.withColumn("latitude", get_lat(df['address']))
            df = df.withColumn("longitude", get_lon(df['address']))
            df = df.withColumn("h3_index", get_h3(df['latitude'], df['longitude']))

        return df
