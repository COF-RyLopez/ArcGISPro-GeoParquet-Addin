# Download pipeline benchmark, 2026-11-25

Cloud Linux container, real Overture S3 data, DuckDB.NET 1.5.5 locally (the 1.5.6 extensions could not be downloaded in this sandbox; the SQL is identical). OSM LayerCake host is blocked from this sandbox.

## Before, provenance on

| Scenario | Files | Rows | Setup | Catalog | Schema | Load | Count | Archive | Display | Discover | Export | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| places-large | 1 | 34,549 | 0.17s | 0.26s | 0.74s | 2.20s | 0.00s | 0.37s | 35.52s | 0.01s | 0.20s | 39.48s |
| buildings-large | 1 | 247,340 | 0.17s | 0.24s | 0.49s | 3.47s | 0.00s | 0.69s | 212.63s | 0.02s | 0.81s | 218.52s |
| segments-large | 1 | 75,813 | 0.17s | 0.23s | 0.56s | 2.25s | 0.00s | 0.65s | 50.22s | 0.01s | 0.30s | 54.40s |

## After, provenance on

| Scenario | Files | Rows | Setup | Catalog | Schema | Load | Count | Archive | Display | Discover | Export | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| places-large | 1 | 34,549 | 0.17s | 0.32s | 0.65s | 1.47s | 0.00s | 0.39s | 0.51s | 0.01s | 0.16s | 3.69s |
| buildings-large | 1 | 247,340 | 0.14s | 0.01s | 0.69s | 3.54s | 0.00s | 0.74s | 0.57s | 0.01s | 0.72s | 6.42s |
| segments-large | 1 | 75,813 | 0.14s | 0.02s | 0.71s | 1.90s | 0.00s | 0.58s | 0.43s | 0.00s | 0.33s | 4.10s |

## Before, provenance off

| Scenario | Files | Rows | Setup | Catalog | Schema | Load | Count | Archive | Display | Discover | Export | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| places-small | 1 | 3,409 | 0.18s | 0.36s | 0.68s | 1.00s | 0.00s | 0.05s | 0.16s | 0.00s | 0.02s | 2.46s |
| buildings-small | 1 | 11,966 | 0.15s | 0.26s | 0.60s | 1.10s | 0.00s | 0.07s | 0.02s | 0.00s | 0.05s | 2.27s |
| segments-small | 1 | 4,232 | 0.14s | 0.01s | 0.56s | 1.59s | 0.00s | 0.05s | 0.01s | 0.00s | 0.02s | 2.38s |
| osm-buildings-small | unreachable from this machine: IO Error: Failure when receiving data from the peer error for HTTP HEAD to 'https://data.openstreetmap.us/layercake/buildings.parquet' |
| places-large | 1 | 34,549 | 0.17s | 0.26s | 0.72s | 1.31s | 0.00s | 0.25s | 1.09s | 0.00s | 0.09s | 3.90s |
| buildings-large | 1 | 247,340 | 0.18s | 0.01s | 0.58s | 3.68s | 0.00s | 0.68s | 0.13s | 0.01s | 0.59s | 5.86s |
| segments-large | 1 | 75,813 | 0.14s | 0.26s | 0.51s | 2.13s | 0.00s | 0.64s | 0.13s | 0.00s | 0.22s | 4.04s |
| osm-buildings-large | unreachable from this machine: IO Error: Failure when receiving data from the peer error for HTTP HEAD to 'https://data.openstreetmap.us/layercake/buildings.parquet' |

## After, provenance off

| Scenario | Files | Rows | Setup | Catalog | Schema | Load | Count | Archive | Display | Discover | Export | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| places-small | 1 | 3,409 | 0.18s | 0.35s | 0.66s | 1.06s | 0.00s | 0.05s | 0.04s | 0.00s | 0.02s | 2.36s |
| buildings-small | 1 | 11,966 | 0.13s | 0.26s | 0.46s | 0.89s | 0.00s | 0.06s | 0.03s | 0.00s | 0.05s | 1.88s |
| segments-small | 1 | 4,232 | 0.15s | 0.26s | 0.47s | 1.66s | 0.00s | 0.06s | 0.01s | 0.00s | 0.02s | 2.63s |
| osm-buildings-small | unreachable from this machine: IO Error: Failure when receiving data from the peer error for HTTP HEAD to 'https://data.openstreetmap.us/layercake/buildings.parquet' |
| places-large | 1 | 34,549 | 0.15s | 0.26s | 0.59s | 1.22s | 0.00s | 0.27s | 0.15s | 0.00s | 0.09s | 2.73s |
| buildings-large | 1 | 247,340 | 0.13s | 0.26s | 0.60s | 3.23s | 0.00s | 0.80s | 0.13s | 0.01s | 0.61s | 5.78s |
| segments-large | 1 | 75,813 | 0.13s | 0.33s | 0.61s | 2.35s | 0.00s | 0.62s | 0.15s | 0.00s | 0.24s | 4.45s |
| osm-buildings-large | unreachable from this machine: IO Error: Failure when receiving data from the peer error for HTTP HEAD to 'https://data.openstreetmap.us/layercake/buildings.parquet' |
