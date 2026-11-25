# GERSify candidate matching, 2026-11-25

`GersSql.Build{Places,Addresses}CandidateTablesCommand` was timed end to end on real Overture 2026-11-18.0 data for the Fresno area. Each run used 3,000 input points sampled from the same data, offset about 6 m. Engine: DuckDB.NET 1.5.5 on 4 CPUs (the 1.5.6 extensions can't be downloaded in this sandbox).

| Target files | Features | Candidates | main | this change |
|---|---|---|---|---|
| Add-in place download (address_* columns) | 63,043 | 12,040 | 11.41s | 3.49s |
| Raw Overture places (addresses list) | 63,043 | 12,040 | 4.33s | 3.36s |
| Raw Overture addresses | 311,816 | 14,647 | 19.12s | 12.77s |

Output: every candidate row matches main, and so does the number of accepted matches per input record. The only difference is the rank between duplicate Overture features whose scores are exactly tied. That ordering already differed between two runs of main (12 rows), so it is not a new behaviour.
