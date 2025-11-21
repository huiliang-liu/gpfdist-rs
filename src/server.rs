use crate::df_engine::{DFEngine, DFRequest, TableType};
use crate::util::{parse_query_map, percent_decode};
use bytes::BytesMut;
use futures::StreamExt;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

// --- Session Management ---

/// Session key identifying a unique logical session based on gpfdist protocol headers
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    xid: String,
    cid: String,
    sn: String,
}

/// Session state tracking
#[derive(Debug, Clone)]
enum SessionStatus {
    InProgress,
    Completed,
}

#[derive(Debug, Clone)]
struct SessionState {
    status: SessionStatus,
    #[allow(dead_code)]
    started_at: Instant,
    completed_at: Option<Instant>,
}

/// Global session manager to track sessions across requests
struct SessionManager {
    sessions: Mutex<HashMap<SessionKey, SessionState>>,
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Check if a session exists and return its status
    async fn get_session_status(&self, key: &SessionKey) -> Option<SessionStatus> {
        let sessions = self.sessions.lock().await;
        sessions.get(key).map(|state| state.status.clone())
    }

    /// Mark a session as in-progress
    async fn mark_in_progress(&self, key: SessionKey) {
        let mut sessions = self.sessions.lock().await;
        sessions.insert(
            key,
            SessionState {
                status: SessionStatus::InProgress,
                started_at: Instant::now(),
                completed_at: None,
            },
        );
    }

    /// Mark a session as completed
    async fn mark_completed(&self, key: &SessionKey) {
        let mut sessions = self.sessions.lock().await;
        if let Some(state) = sessions.get_mut(key) {
            state.status = SessionStatus::Completed;
            state.completed_at = Some(Instant::now());
        }
    }

    /// Evict old completed sessions (TTL-based cleanup)
    /// Remove sessions completed more than the given duration ago
    async fn evict_old_sessions(&self, ttl: Duration) {
        let mut sessions = self.sessions.lock().await;
        let now = Instant::now();
        sessions.retain(|_key, state| {
            match state.status {
                SessionStatus::Completed => {
                    if let Some(completed_at) = state.completed_at {
                        // Keep if not yet expired
                        now.duration_since(completed_at) < ttl
                    } else {
                        // Keep sessions without completed_at timestamp
                        true
                    }
                }
                SessionStatus::InProgress => {
                    // Keep InProgress sessions
                    true
                }
            }
        });
    }
}

// Global static session manager
static SESSION_MANAGER: Lazy<SessionManager> = Lazy::new(|| SessionManager::new());

/// Extract session key from headers (X-GP-XID, X-GP-CID, X-GP-SN)
fn extract_session_key(headers: &std::collections::HashMap<String, String>) -> Option<SessionKey> {
    let xid = headers.get("x-gp-xid")?.clone();
    let cid = headers.get("x-gp-cid")?.clone();
    let sn = headers.get("x-gp-sn")?.clone();
    Some(SessionKey { xid, cid, sn })
}

/// Send an immediate EOF response for cached/completed sessions
async fn send_immediate_eof_response(
    socket: &mut TcpStream,
    gp_proto: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    // Send HTTP OK header
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/octet-stream\r\n\
         X-GP-PROTO: {}\r\n\
         Connection: close\r\n\
         \r\n",
        gp_proto
    );
    socket.write_all(response.as_bytes()).await?;

    // For protocol 1, send minimal framing (just EOF)
    if gp_proto == 1 {
        // Send EOF frame (zero-length D frame)
        socket.write_all(&frame_eof_bytes()).await?;
    }
    // For protocol 0, just close connection (no data)

    Ok(())
}

/// Create a unique table name for each request to avoid collisions
/// Format: {source}_seg{sid}_{nanos}
/// Only letters, digits, and underscores are allowed in the identifier
fn make_unique_table_name(source: &str, segid: Option<usize>) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get current system time")
        .as_nanos();

    let sid = segid.unwrap_or(0);
    let sanitized_source: String = source
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    format!("{}_seg{}_{}", sanitized_source, sid, nanos)
}

pub struct Server {
    addr: String,
    df_engine: Arc<DFEngine>,
}

impl Server {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            df_engine: Arc::new(DFEngine::new()),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("gpfdist-rs listening on {}", self.addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let df_engine = Arc::clone(&self.df_engine);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, df_engine).await {
                    eprintln!("Error handling connection: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    df_engine: Arc<DFEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = vec![0; 8192];
    let n = socket.read(&mut buf).await?;

    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let lines: Vec<&str> = request.lines().collect();

    if lines.is_empty() {
        send_error(&mut socket, 400, "Bad Request").await?;
        return Ok(());
    }

    // Parse request line
    let parts: Vec<&str> = lines[0].split_whitespace().collect();
    if parts.len() < 2 {
        send_error(&mut socket, 400, "Bad Request").await?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    // Parse headers
    let mut headers = std::collections::HashMap::new();
    for line in lines.iter().skip(1) {
        if line.is_empty() {
            break;
        }
        if let Some(colon_pos) = line.find(':') {
            let key = line[..colon_pos].trim().to_lowercase();
            let value = line[colon_pos + 1..].trim().to_string();
            headers.insert(key, value);
        }
    }

    // Parse X-GP-PROTO header
    let gp_proto = headers.get("x-gp-proto").and_then(|s| s.parse::<u8>().ok());

    // Enforce X-GP-PROTO protocol mapping rules
    match method {
        "GET" => {
            // For GET requests, X-GP-PROTO MUST be 1
            match gp_proto {
                Some(1) => {
                    // Valid GET with gp_proto=1
                    if path.starts_with("/df/") {
                        handle_df_route(socket, path, &headers, df_engine).await?;
                    } else {
                        // Fallback file serving for non-/df/ paths
                        handle_file_route(socket, path).await?;
                    }
                }
                _ => {
                    // Missing or invalid X-GP-PROTO for GET
                    send_error(&mut socket, 400, "X-GP-PROTO must be 1 for GET").await?;
                }
            }
        }
        "POST" => {
            // For POST requests, X-GP-PROTO MUST be 0
            match gp_proto {
                Some(0) => {
                    // Valid POST with gp_proto=0, but not yet supported
                    send_error(&mut socket, 400, "only GET supported currently").await?;
                }
                _ => {
                    // Missing or invalid X-GP-PROTO for POST
                    send_error(&mut socket, 400, "X-GP-PROTO must be 0 for POST").await?;
                }
            }
        }
        _ => {
            // Other methods are not supported
            send_error(&mut socket, 400, "unsupported method").await?;
        }
    }

    Ok(())
}

async fn handle_df_route(
    socket: TcpStream,
    path: &str,
    headers: &std::collections::HashMap<String, String>,
    df_engine: Arc<DFEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract source type from path: /df/{source}
    let path_without_query = path.split('?').next().unwrap_or(path);
    let parts: Vec<&str> = path_without_query.split('/').collect();

    // Need a mutable socket for sending error responses before wrapping in BufWriter
    let mut raw_socket = socket;

    if parts.len() < 3 {
        send_error(&mut raw_socket, 400, "Invalid /df/ route").await?;
        return Ok(());
    }

    let source = parts[2];

    // Parse table type
    let table_type = match source {
        "parquet" => TableType::Parquet,
        #[cfg(feature = "delta")]
        "delta" => TableType::Delta,
        #[cfg(not(feature = "delta"))]
        "delta" => {
            send_error(&mut raw_socket, 400, "Delta feature not enabled").await?;
            return Ok(());
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => TableType::Iceberg,
        #[cfg(not(feature = "iceberg"))]
        "iceberg" => {
            send_error(&mut raw_socket, 400, "Iceberg feature not enabled").await?;
            return Ok(());
        }
        _ => {
            send_error(&mut raw_socket, 400, "Unknown source type").await?;
            return Ok(());
        }
    };

    // Parse query parameters
    let query_map = parse_query_map(path);

    // Extract parameters
    let uri = query_map.get("path").map(|s| s.clone());
    let files_str = query_map.get("files").map(|s| s.clone());
    let columns_str = query_map.get("columns").map(|s| s.clone());
    let filter_str = query_map.get("filter").map(|s| s.clone());
    let limit_str = query_map.get("limit");

    // Validate: must have either path or files
    if uri.is_none() && files_str.is_none() {
        send_error(&mut raw_socket, 400, "Missing 'path' or 'files' parameter").await?;
        return Ok(());
    }

    // Parse file list
    let file_list = if let Some(files) = files_str {
        let decoded = percent_decode(&files);
        Some(decoded.split(',').map(|s| s.trim().to_string()).collect())
    } else {
        None
    };

    // Parse projection
    let projection = if let Some(cols) = columns_str {
        let decoded = percent_decode(&cols);
        Some(decoded.split(',').map(|s| s.trim().to_string()).collect())
    } else {
        None
    };

    // Parse filter
    let filter = filter_str.map(|f| percent_decode(&f));

    // Parse limit
    let limit = limit_str.and_then(|s| s.parse::<usize>().ok());

    // Parse headers
    let gp_proto = headers
        .get("x-gp-proto")
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);

    if gp_proto > 1 {
        send_error(&mut raw_socket, 400, "Invalid X-GP-PROTO (must be 0 or 1)").await?;
        return Ok(());
    }

    let segment_id = headers
        .get("x-gp-segment-id")
        .and_then(|s| s.parse::<usize>().ok());

    let segment_count = headers
        .get("x-gp-segment-count")
        .and_then(|s| s.parse::<usize>().ok());

    // Session management: Extract session headers
    let session_key = extract_session_key(headers);

    // Session-aware data sources: delta, iceberg (but not parquet with explicit file list)
    // Parquet with file_list uses segmentation on file list, so no session caching
    let use_session_caching = match table_type {
        #[cfg(feature = "delta")]
        TableType::Delta => file_list.is_none(),
        #[cfg(feature = "iceberg")]
        TableType::Iceberg => file_list.is_none(),
        TableType::Parquet => false, // Parquet uses file-level segmentation
    };

    // If session headers are present and this source supports session caching, check session state
    if let Some(key) = &session_key {
        if use_session_caching {
            // Perform TTL eviction on each request (opportunistic cleanup)
            SESSION_MANAGER
                .evict_old_sessions(Duration::from_secs(5 * 60))
                .await;

            // Check if session already exists
            if let Some(status) = SESSION_MANAGER.get_session_status(key).await {
                match status {
                    SessionStatus::InProgress => {
                        // Session is in progress by another request; return immediate EOF
                        eprintln!(
                            "Session {:?} already in progress, returning immediate EOF",
                            key
                        );
                        send_immediate_eof_response(&mut raw_socket, gp_proto).await?;
                        return Ok(());
                    }
                    SessionStatus::Completed => {
                        // Session already completed; return immediate EOF
                        eprintln!("Session {:?} already completed, returning immediate EOF", key);
                        send_immediate_eof_response(&mut raw_socket, gp_proto).await?;
                        return Ok(());
                    }
                }
            } else {
                // New session: mark as in-progress
                eprintln!("New session {:?}, marking as in-progress", key);
                SESSION_MANAGER.mark_in_progress(key.clone()).await;
            }
        }
    }

    // Generate unique table name for directory mode (when uri is provided but file_list is not)
    let table_name = if uri.is_some() && file_list.is_none() {
        Some(make_unique_table_name(source, segment_id))
    } else {
        None
    };

    // Build request
    let request = DFRequest {
        table_type,
        uri: uri.unwrap_or_default(),
        file_list,
        projection,
        filter,
        limit,
        segment_id,
        segment_count,
        gp_proto,
        table_name,
    };

    // Execute query and stream results
    match df_engine.execute_csv_batches(request).await {
        Ok(mut stream) => {
            // Send success headers
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/octet-stream\r\n\
                 X-GP-PROTO: {}\r\n\
                 Connection: close\r\n\
                 \r\n",
                gp_proto
            );
            raw_socket.write_all(response.as_bytes()).await?;

            // Optimization 1: Wrap socket in BufWriter for fewer syscalls and better throughput.
            // Using 64KB buffer size.
            let mut writer = BufWriter::with_capacity(64 * 1024, raw_socket);

            // Protocol 1 requires framing at the server layer
            if gp_proto == 1 {
                // State for framing: first_batch flag, offset, line_no
                let mut first_batch = true;
                let mut _offset: u64 = 0;
                let mut _line_no: u64 = 1;

                // Stream data with server-side framing
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(csv_bytes) => {
                            // On first batch only: emit F, O, L frames
                            if first_batch {
                                // Optimization 2: Write frames directly to writer without creating BytesMut intermediate buffers
                                writer.write_all(&frame_f_bytes(&source)).await?;
                                writer.write_all(&frame_o_bytes(0)).await?;
                                writer.write_all(&frame_l_bytes(1)).await?;
                                first_batch = false;
                            }

                            // Count lines in this batch
                            let newline_count = bytecount::count(&csv_bytes, b'\n');
                            let rows_in_batch = if csv_bytes.is_empty()
                                || csv_bytes[csv_bytes.len() - 1] == b'\n'
                            {
                                newline_count as u64
                            } else {
                                // Last line doesn't end with newline, add 1
                                newline_count as u64 + 1
                            };

                            // Optimization 3: Zero-Copy Data Framing
                            // 1. Write the fixed 5-byte frame header
                            let data_len = csv_bytes.len() as u32;
                            let header = frame_hdr_bytes(b'D', data_len);
                            writer.write_all(&header).await?;

                            // 2. Write the CSV payload directly from the reference
                            writer.write_all(&csv_bytes).await?;

                            // Update state
                            _offset += csv_bytes.len() as u64;
                            _line_no += rows_in_batch;
                        }
                        Err(e) => {
                            eprintln!("Stream error: {}", e);
                            // Send error frame + EOF
                            let err_msg = format!("ERROR: {}", e);
                            writer.write_all(&frame_e_bytes(&err_msg)).await?;
                            writer.write_all(&frame_eof_bytes()).await?;
                            writer.flush().await?;
                            return Ok(());
                        }
                    }
                }

                // After stream ends: emit EOF (zero-length D frame)
                writer.write_all(&frame_eof_bytes()).await?;
                writer.flush().await?;
            } else {
                // Protocol 0: raw CSV only (no framing)
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            writer.write_all(&chunk).await?;
                        }
                        Err(e) => {
                            eprintln!("Stream error: {}", e);
                            break;
                        }
                    }
                }
                writer.flush().await?;
            }

            // Mark session as completed after successful streaming
            if let Some(key) = &session_key {
                if use_session_caching {
                    eprintln!("Session {:?} completed successfully", key);
                    SESSION_MANAGER.mark_completed(key).await;
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to execute query: {}", e);
            send_error(
                &mut raw_socket,
                500,
                &format!("Query execution failed: {}", e),
            )
            .await?;
        }
    }

    Ok(())
}

async fn handle_file_route(
    mut socket: TcpStream,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Split path and query string
    let (file_path, query_str) = if let Some(pos) = path.find('?') {
        (&path[..pos], Some(&path[pos + 1..]))
    } else {
        (path, None)
    };

    // Percent-decode the file path
    let decoded_path = percent_decode(file_path);

    // Security: reject paths containing ".." to prevent directory traversal
    if decoded_path.contains("..") {
        send_error(&mut socket, 400, "Path traversal not allowed").await?;
        return Ok(());
    }

    // Parse query parameters for "lines" parameter
    let lines_limit = if let Some(query) = query_str {
        let query_map = parse_query_map(&format!("?{}", query));
        query_map.get("lines").and_then(|s| s.parse::<usize>().ok())
    } else {
        None
    };

    // Resolve path, the path is always relative to current working directory
    let resolved_path = std::env::current_dir()?.join(&decoded_path.trim_start_matches('/'));

    // Optimization: Open file for streaming instead of reading entire file into memory
    let mut file = match File::open(&resolved_path).await {
        Ok(f) => f,
        Err(e) => {
            // Send error frame
            let err_msg = format!(
                "Failed to read file: {}, error {}",
                resolved_path.display(),
                e
            );
            eprintln!("{}", err_msg);

            // Send HTTP header with X-GP-PROTO: 1
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/octet-stream\r\n\
                 X-GP-PROTO: 1\r\n\
                 Connection: close\r\n\
                 \r\n"
            );
            socket.write_all(response.as_bytes()).await?;

            // Send E frame + EOF
            socket.write_all(&frame_e_bytes(&err_msg)).await?;
            socket.write_all(&frame_eof_bytes()).await?;
            return Ok(());
        }
    };

    // Send HTTP header with X-GP-PROTO: 1
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/octet-stream\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n"
    );
    socket.write_all(response.as_bytes()).await?;

    // Use BufWriter for efficient sending
    let mut writer = BufWriter::with_capacity(64 * 1024, socket);

    // Send initial frames: F, O, L
    writer.write_all(&frame_f_bytes(&decoded_path)).await?;
    writer.write_all(&frame_o_bytes(0)).await?;
    writer.write_all(&frame_l_bytes(1)).await?;

    // Optimization: Stream file content using a buffer
    let mut buf = vec![0u8; 32 * 1024]; // 32KB buffer
    let mut lines_sent = 0;
    let mut stop_streaming = false;

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break; // EOF
        }

        let mut chunk = &buf[..n];

        // Handle lines limit if specified
        if let Some(limit) = lines_limit {
            let newlines_in_chunk = bytecount::count(chunk, b'\n');

            if lines_sent + newlines_in_chunk >= limit {
                // We have reached or exceeded the limit in this chunk
                let needed = limit - lines_sent;

                // Find the position of the 'needed'-th newline
                let mut pos = 0;
                let mut found = 0;
                for (i, &b) in chunk.iter().enumerate() {
                    if b == b'\n' {
                        found += 1;
                        if found == needed {
                            pos = i + 1; // Include the newline
                            break;
                        }
                    }
                }

                // Truncate the chunk
                if pos > 0 {
                    chunk = &buf[..pos];
                }
                stop_streaming = true;
            }
            lines_sent += newlines_in_chunk;
        }

        // Write D Frame Header
        let header = frame_hdr_bytes(b'D', chunk.len() as u32);
        writer.write_all(&header).await?;
        // Write data
        writer.write_all(chunk).await?;

        if stop_streaming {
            break;
        }
    }

    // Send EOF Frame
    writer.write_all(&frame_eof_bytes()).await?;
    writer.flush().await?;

    Ok(())
}

// --- Optimized Framing Helpers (Zero-allocation for headers) ---

/// Create a fixed 5-byte frame header array
fn frame_hdr_bytes(letter: u8, len: u32) -> [u8; 5] {
    let mut buf = [0u8; 5];
    buf[0] = letter;
    buf[1..5].copy_from_slice(&len.to_be_bytes());
    buf
}

/// Create an 'F' (filename) frame as BytesMut (still used for variable length)
fn frame_f_bytes(filename: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + filename.len());
    buf.extend_from_slice(&frame_hdr_bytes(b'F', filename.len() as u32));
    buf.extend_from_slice(filename.as_bytes());
    buf
}

/// Create an 'O' (offset) frame as BytesMut
fn frame_o_bytes(offset: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(17); // 9 header + 8 data
                                               // Note: The protocol uses the 4-byte line_or_offset field for small offsets,
                                               // but O frames often carry the full 64-bit offset in the data payload depending on version.
                                               // Standard gpfdist usually puts 0 in header and 8-byte offset in data for O frames.
    buf.extend_from_slice(&frame_hdr_bytes(b'O', 8));
    buf.extend_from_slice(&offset.to_be_bytes());
    buf
}

/// Create an 'L' (line number) frame as BytesMut
fn frame_l_bytes(line_no: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(17); // 9 header + 8 data
    buf.extend_from_slice(&frame_hdr_bytes(b'L', 8));
    buf.extend_from_slice(&line_no.to_be_bytes());
    buf
}

/// Create an 'E' (error) frame as BytesMut
fn frame_e_bytes(msg: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(9 + msg.len());
    buf.extend_from_slice(&frame_hdr_bytes(b'E', msg.len() as u32));
    buf.extend_from_slice(msg.as_bytes());
    buf
}

/// Create an EOF frame bytes
fn frame_eof_bytes() -> [u8; 5] {
    frame_hdr_bytes(b'D', 0)
}

async fn send_error(
    socket: &mut TcpStream,
    status: u16,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let status_text = match status {
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "Error",
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status,
        status_text,
        message.len(),
        message
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}
