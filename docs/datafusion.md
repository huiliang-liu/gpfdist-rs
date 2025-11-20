# DataFusion Integration

This document describes the DataFusion integration in gpfdist-rs, which allows serving query-filtered and projected tabular data over the gpfdist protocol.

## Overview

The `/df/{source}` endpoint provides access to tabular data stored in various formats (Parquet, Delta Lake, Iceberg) with support for:
- Column projection
- SQL-based filtering
- Row limits
- Segment-based partitioning for parallel processing
- gpfdist protocol versions 0 and 1

## Endpoint

```
GET /df/{source}
```

Where `{source}` is one of:
- `parquet` - Query Parquet files or directories
- `delta` - Query Delta Lake tables (requires `delta` feature)
- `iceberg` - Query Iceberg tables (requires `iceberg` feature)

## Query Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `path` | Conditional* | Directory path for Parquet/Delta/Iceberg table | `path=/data/table` |
| `files` | Conditional* | Comma-separated list of specific files to query | `files=/data/file1.parquet,/data/file2.parquet` |
| `columns` | Optional | Comma-separated list of columns to project | `columns=id,name,age` |
| `filter` | Optional | URL-encoded SQL WHERE clause predicate | `filter=age%20%3E%2018` (age > 18) |
| `limit` | Optional | Maximum number of rows to return | `limit=1000` |

\* Either `path` or `files` must be provided.

### Notes on Query Parameters

- **Path vs Files**: Use `path` for directory-based queries (all files in directory), or `files` for explicit file lists.
- **Segmentation**: When `files` is used with segment headers, files are distributed across segments using modulo partitioning.
- **Projection**: If `columns` is not specified, all columns are returned (SELECT *).
- **Filter**: The filter parameter must be URL-encoded. It should be a valid SQL predicate expression.

## HTTP Headers

### Request Headers

| Header | Required | Description | Example |
|--------|----------|-------------|---------|
| `X-GP-PROTO` | Optional | Protocol version: 0 (raw CSV) or 1 (framed) | `X-GP-PROTO: 1` |
| `X-GP-SEGMENT-ID` | Optional | Segment identifier (0-based) for parallel processing | `X-GP-SEGMENT-ID: 0` |
| `X-GP-SEGMENT-COUNT` | Optional | Total number of segments | `X-GP-SEGMENT-COUNT: 4` |

Default values:
- `X-GP-PROTO`: 0
- Segmentation is disabled if either segment header is missing

### Response Headers

| Header | Description |
|--------|-------------|
| `X-GP-PROTO` | Echoes the protocol version used |
| `Content-Type` | `application/octet-stream` |
| `Connection` | `close` |

## Protocol Formats

### Protocol 0 (Raw CSV)

Returns data as plain CSV with no framing:
```
1,Alice,30
2,Bob,25
3,Charlie,35
```

### Protocol 1 (Framed)

Returns data with gpfdist framing protocol:

**Frame Structure:**
```
[Type:1][Length:4][Line/Offset:4][Data:Length]
```

**Frame Types:**
- `F` (File header): Sent once at the beginning
- `O` (Offset): Current byte offset in output
- `L` (Line number): Current line number (1-based)
- `D` (Data): CSV data batch
- `E` (Error): Error message (on failure)

**Frame Sequence:**
1. F frame (once at start)
2. For each batch:
   - O frame (byte offset)
   - L frame (line number)
   - D frame (CSV data)
3. EOF: D frame with length 0

**Example Frame Sequence:**
```
F [0, 0, 0, 0] [0, 0, 0, 0]          # File header
O [0, 0, 0, 0] [0, 0, 0, 0]          # Offset 0
L [0, 0, 0, 0] [0, 0, 0, 1]          # Line 1
D [0, 0, 0, 30] [0, 0, 0, 30] [CSV]  # 30 bytes of CSV
D [0, 0, 0, 0] [0, 0, 0, 0]          # EOF
```

## Feature Flags

The DataFusion integration supports optional features that must be enabled at compile time:

```toml
[features]
default = []
delta = ["deltalake"]
iceberg = ["iceberg-rust"]
```

### Building with Features

**Default (Parquet only):**
```bash
cargo build
```

**With Delta Lake support:**
```bash
cargo build --features delta
```

**With all features:**
```bash
cargo build --features delta,iceberg
```

### Feature Availability

When a feature is not enabled:
- Requests to disabled endpoints return HTTP 400 with an explanatory message
- Example: `/df/delta` without `delta` feature → "Delta feature not enabled"

## Segmentation

Segmentation allows parallel processing by distributing files across multiple segments:

**How it works:**
1. Client specifies segment ID and count in headers
2. Server filters files using: `file_index % segment_count == segment_id`
3. Each segment processes a disjoint subset of files
4. Union of all segments equals the complete dataset

**Example:**
```
Files: [file0, file1, file2, file3]
Segments: 2 (segment_count=2)

Segment 0 (segment_id=0): file0, file2  (indices 0, 2)
Segment 1 (segment_id=1): file1, file3  (indices 1, 3)
```

**Usage:**
```bash
# Segment 0
curl -H "X-GP-SEGMENT-ID: 0" \
     -H "X-GP-SEGMENT-COUNT: 2" \
     "http://localhost:8080/df/parquet?files=file0.parquet,file1.parquet,file2.parquet,file3.parquet"

# Segment 1  
curl -H "X-GP-SEGMENT-ID: 1" \
     -H "X-GP-SEGMENT-COUNT: 2" \
     "http://localhost:8080/df/parquet?files=file0.parquet,file1.parquet,file2.parquet,file3.parquet"
```

## Examples

### Basic Parquet Query

```bash
curl "http://localhost:8080/df/parquet?path=/data/sales"
```

### Query with Projection and Filter

```bash
curl "http://localhost:8080/df/parquet?path=/data/sales&columns=product,revenue&filter=revenue%20%3E%201000&limit=100"
```

### Query Specific Files with Protocol 1

```bash
curl -H "X-GP-PROTO: 1" \
     "http://localhost:8080/df/parquet?files=/data/sales/part-0.parquet,/data/sales/part-1.parquet"
```

### Delta Lake Query (with delta feature)

```bash
curl "http://localhost:8080/df/delta?path=/data/delta_table&columns=id,name&limit=50"
```

### Segmented Query

```bash
# Process files in parallel across 4 segments
for i in {0..3}; do
    curl -H "X-GP-SEGMENT-ID: $i" \
         -H "X-GP-SEGMENT-COUNT: 4" \
         "http://localhost:8080/df/parquet?files=/data/file0.parquet,/data/file1.parquet,/data/file2.parquet,/data/file3.parquet" \
         > segment_$i.csv &
done
wait
```

## Error Handling

| Scenario | Response | Behavior |
|----------|----------|----------|
| Invalid gp_proto (not 0 or 1) | HTTP 400 | Returns error message |
| Missing path and files | HTTP 400 | Returns "Missing 'path' or 'files' parameter" |
| Feature not enabled | HTTP 400 | Returns feature-specific error message |
| Query execution error (proto 0) | HTTP 500 | Returns error message |
| Query execution error (proto 1) | HTTP 200 | Sends E frame + EOF frame |

## Performance Considerations

1. **Batch Size**: DataFusion processes data in batches; CSV encoding happens per batch in blocking threads
2. **Projection Pushdown**: Specifying columns reduces I/O by reading only needed columns from Parquet
3. **Predicate Pushdown**: DataFusion automatically pushes filters down when possible
4. **Segmentation**: Use segmentation for parallel processing of large file sets
5. **Memory**: Large result sets stream incrementally; avoid excessive limits without segmentation

## Limitations

- Arrow IPC streaming (future gp_proto=2) is not implemented
- Advanced predicate pushdown beyond DataFusion defaults is not available
- Delta/Iceberg version/timestamp pinning is not supported (always uses latest snapshot)
- No authentication or TLS support
- Iceberg support is a placeholder and not fully functional

## Testing

Integration tests are provided but marked as `#[ignore]` by default:

```bash
# Run all tests (skips ignored)
cargo test

# Run integration tests (requires test environment)
cargo test -- --ignored

# Run specific test
cargo test --test df_parquet -- --ignored
```

Test coverage includes:
- `tests/df_parquet.rs`: Parquet queries with proto 0/1, projection, filter, limit
- `tests/df_delta.rs`: Delta Lake queries (when delta feature enabled)
- `tests/df_iceberg.rs`: Iceberg placeholder tests
- `tests/seg_partition.rs`: Segmentation correctness (disjoint subsets, complete union)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GPFDIST_ADDR` | `0.0.0.0:8080` | Server bind address and port |

## See Also

- [DataFusion Documentation](https://arrow.apache.org/datafusion/)
- [Delta Lake Documentation](https://delta.io/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Greenplum gpfdist Protocol](https://docs.vmware.com/en/VMware-Greenplum/index.html)
