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
