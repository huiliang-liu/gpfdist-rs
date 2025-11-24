# gpfdist Protocol Documentation

This document describes the gpfdist protocol implementation in gpfdist-rs, focusing on the framing protocol (gp_proto=1).

## Protocol Versions

### gp_proto=0 (Raw CSV)
- Data is streamed as raw CSV bytes without any framing
- Used for POST operations (write mode)
- Simple byte stream with no metadata

### gp_proto=1 (Framed Protocol)
- Data is wrapped in a structured frame format
- Each frame has a type indicator and length prefix
- Used for GET operations (read mode)
- Provides metadata about file position and line numbers

## Frame Structure

All frames follow this structure:
```
[Type:1 byte][Length:4 bytes big-endian][Data:Length bytes]
```

### Frame Types

#### F Frame (Filename)
- Type: `0x46` ('F')
- Data: UTF-8 encoded filename/source identifier
- Purpose: Identifies the data source

#### O Frame (Offset)
- Type: `0x4F` ('O')
- Data: 8 bytes (u64 big-endian)
- Purpose: Cumulative byte offset of CSV payload
- **Important**: Offset counts only CSV data bytes, not frame headers or metadata

#### L Frame (Line Number)
- Type: `0x4C` ('L')
- Data: 8 bytes (u64 big-endian)
- Purpose: Current line number (1-indexed)
- **Important**: Only counts completed lines (lines ending with '\n')
- Partial lines at EOF are counted when the EOF packet is sent

#### D Frame (Data)
- Type: `0x44` ('D')
- Data: CSV payload bytes
- Purpose: Contains the actual CSV data
- **Special case**: Zero-length D frame signals EOF

#### E Frame (Error)
- Type: `0x45` ('E')
- Data: UTF-8 encoded error message
- Purpose: Reports errors during data processing

## Packet Structure

### Normal Data Packet
Every data packet consists of F/O/L meta frames followed by a D frame:
```
F[filename] O[offset] L[line_no] D[csv_data]
```

### EOF Packet
End of stream is signaled by F/O/L meta frames followed by zero-length D:
```
F[filename] O[final_offset] L[final_line_no] D[]
```

### Error Packet
Errors are reported with two sets of meta frames:
```
F[filename] O[offset] L[line_no] E[error_msg]
F[filename] O[offset] L[line_no] D[]
```

## Per-Packet Meta Frames

By default, gpfdist-rs emits F/O/L meta frames **before every packet**, including:
- Each data packet (D frame with payload)
- The EOF packet (zero-length D frame)
- Error packets (E frame followed by EOF)

This ensures that downstream consumers always have complete metadata for each chunk of data.

### Compact Meta Mode

When the `compact-meta` feature flag is enabled, gpfdist-rs operates in legacy mode:
- F/O/L frames are emitted only once at the start
- Subsequent packets contain only D frames
- Final EOF packet includes F/O/L frames

To enable:
```bash
cargo build --features compact-meta
```

## Offset and Line Number Semantics

### Offset Calculation
- Starts at 0
- Increments by the size of each CSV data chunk
- **Does not include**: Frame headers (5 bytes each), meta frame payloads, or any protocol overhead
- Represents the cumulative count of actual CSV bytes transmitted

### Line Number Calculation
- Starts at 1 (first line)
- Increments by the count of newline characters (`\n`) in each chunk
- **Partial lines**: If a chunk doesn't end with `\n`, the partial line is NOT counted immediately
- At EOF: If the last chunk didn't end with `\n` and data was sent, the final partial line is counted
- This ensures line numbers represent **completed** lines, matching Greenplum gpfdist expectations

Example:
```
Chunk 1: "line1\nline2\n" → offset=14, line_no=3 (started at 1, saw 2 newlines)
Chunk 2: "line3" → offset=19, line_no=3 (no newlines, partial line not counted yet)
EOF: offset=19, line_no=4 (final partial line counted at EOF)
```

## Session Management and Short-Circuit Responses

### Session Identification
Sessions are identified by three headers:
- `X-GP-XID`: Transaction ID
- `X-GP-CID`: Command ID  
- `X-GP-SN`: Segment number

### Session Short-Circuit Behavior

When a session is repeated (same XID/CID/SN):

#### First Request (Primary Reader)
- Processes normally
- Streams data with F/O/L + D packets
- Completes with F/O/L + EOF

#### Subsequent Concurrent Requests
- Immediately return F/O/L + EOF without reading data
- Use the original source name (not "repeat_session")
- Optionally include cached error (see Error Caching)

### Error Caching

When `GPFDIST_SESSION_REPEAT_ERROR=true` environment variable is set:
- First error encountered in a session is cached
- Subsequent requests for the same session receive:
  ```
  F[source] O[0] L[1] E[cached_error]
  F[source] O[0] L[1] D[]
  ```

When disabled (default):
- Subsequent requests receive only:
  ```
  F[source] O[0] L[1] D[]
  ```

## Example Protocol Flow

### Successful Query
```
HTTP/1.1 200 OK
Content-Type: application/octet-stream
X-GP-PROTO: 1
Connection: close

F[8]parquet O[8]0 L[8]1 D[100]<csv data 1>
F[8]parquet O[8]100 L[8]5 D[150]<csv data 2>
F[8]parquet O[8]250 L[8]10 D[0]
```

### Query with Error
```
HTTP/1.1 200 OK
Content-Type: application/octet-stream
X-GP-PROTO: 1
Connection: close

F[8]parquet O[8]0 L[8]1 D[100]<csv data>
F[8]parquet O[8]100 L[8]5 E[30]ERROR: Query execution failed
F[8]parquet O[8]100 L[8]5 D[0]
```

## Implementation Notes

### Helper Functions
The implementation provides helper functions for consistent framing:
- `write_meta(writer, filename, offset, line_no)` - Writes F/O/L frames
- `write_data_packet(writer, filename, offset, line_no, csv_bytes)` - Complete data packet
- `write_error_packet(writer, filename, offset, line_no, err_msg)` - Error packet
- `write_eof_packet(writer, filename, offset, line_no)` - EOF packet

### Line Number State Tracking
The implementation maintains:
- `offset`: Cumulative CSV byte count
- `line_no`: Current completed line number (1-indexed)
- `last_chunk_ended_with_newline`: Boolean flag to handle partial lines correctly

### Frame Header Construction
```rust
fn frame_hdr_bytes(letter: u8, len: u32) -> [u8; 5] {
    let mut b = [0u8; 5];
    b[0] = letter;
    b[1..5].copy_from_slice(&len.to_be_bytes());
    b
}
```

## Compatibility

This implementation is designed to be compatible with Greenplum Database's gpfdist protocol expectations. The per-packet meta frames ensure that:

1. Each data chunk can be independently parsed
2. Position information is always available
3. Error handling is consistent
4. Session management prevents duplicate reads

For legacy systems expecting single F/O/L frames, use the `compact-meta` feature flag.
