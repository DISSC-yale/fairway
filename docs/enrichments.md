# Geospatial Enrichments

**fairway** includes built-in support for geospatial data enrichment, making it easy to turn raw address or coordinate data into analysis-ready spatial indices.

## Geocoding

When enabled, fairway can resolve address fields to latitude and longitude coordinates.

*   **Enable in Config**:
    ```yaml
    enrichment:
      geocode: true
    ```

## H3 Indexing

For efficient spatial joining and aggregation, fairway can assign **H3 indices** (Hexagonal Hierarchical Spatial Index) to your data.

*   **Why H3?**: Hexagons minimize quantization error when aggregating spatial data and have uniform distances between neighbors.

## Geo-Digest

The pipeline automatically generates a "geo-digest" – a summary of the spatial distribution of your data, identifying potential outliers or data density issues.
