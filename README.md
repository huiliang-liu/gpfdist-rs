# gpfdist-rs

A high-performance gpfdist-compatible server implemented in Rust with DataFusion integration for querying Parquet, Delta Lake, and Iceberg tables.

## Features

- **DataFusion Integration**: Query tabular data using Apache Arrow DataFusion
- **Multiple Formats**: Support for Parquet (default), Delta Lake, and Iceberg
- **Query Capabilities**: 
  - Column projection
  - SQL-based filtering
  - Row limits
  - Parallel processing via segmentation
- **Protocol Support**:
  - gp_proto 0: Raw CSV output
  - gp_proto 1: Framed output with F/O/L/D protocol
- **Feature Flags**: Optional Delta Lake and Iceberg support

## Quick Start

### Build

```bash
# Default build (Parquet support only)
cargo build --release

# With Delta Lake support
cargo build --release --features delta

# With all features
cargo build --release --features delta,iceberg
```

### Run

```bash
# Start server on default port (8080)
cargo run --release

# Custom address
GPFDIST_ADDR="0.0.0.0:9000" cargo run --release
```

### Example Query

```bash
# Query a parquet file with projection and filter
curl -H "X-GP-PROTO: 0" \
  "http://localhost:8080/df/parquet?files=/data/sales.parquet&columns=id,name&filter=id%20%3E%2010&limit=100"
```

## API Endpoints

### `/df/{source}`

Query tabular data where `{source}` is one of:
- `parquet` - Query Parquet files
- `delta` - Query Delta Lake tables (requires `delta` feature)
- `iceberg` - Query Iceberg tables (requires `iceberg` feature)

#### Query Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `path` | Directory path or table location | `path=/data/table` |
| `files` | Comma-separated file list | `files=file1.parquet,file2.parquet` |
| `columns` | Column projection | `columns=id,name,age` |
| `filter` | SQL WHERE clause (URL-encoded) | `filter=age%20%3E%2018` |
| `limit` | Maximum rows | `limit=1000` |

#### Headers

| Header | Description | Default |
|--------|-------------|---------|
| `X-GP-PROTO` | Protocol version (0 or 1) | 0 |
| `X-GP-SEGMENT-ID` | Segment ID for parallel processing | - |
| `X-GP-SEGMENT-COUNT` | Total segment count | - |

## Documentation

For detailed documentation, see:
- [DataFusion Integration Guide](docs/datafusion.md)

## Testing

```bash
# Run unit tests
cargo test

# Run integration tests (requires environment setup)
cargo test -- --ignored
```

## Architecture

```
src/
├── main.rs          # Entry point
├── lib.rs           # Library exports
├── server.rs        # HTTP server and routing
├── df_engine.rs     # DataFusion query engine
└── util.rs          # Utility functions

tests/
├── df_parquet.rs    # Parquet integration tests
├── df_delta.rs      # Delta Lake integration tests
├── df_iceberg.rs    # Iceberg integration tests
└── seg_partition.rs # Segmentation tests
```

## Requirements

- Rust 1.70 or later
- Cargo

## Feature Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `delta` | Enable Delta Lake support | deltalake |
| `iceberg` | Enable Iceberg support | iceberg-rust |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GPFDIST_ADDR` | `0.0.0.0:8080` | Server bind address |

## Performance

- Streaming data processing with minimal memory footprint
- Parallel CSV encoding in blocking threads
- Automatic predicate and projection pushdown via DataFusion
- Segmentation for parallel client-side processing

## Limitations

- Arrow IPC streaming not yet supported
- Iceberg support is a placeholder
- No authentication or TLS
- Delta/Iceberg always use latest snapshot

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
