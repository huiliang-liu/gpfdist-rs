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

use memchr::memmem;

// ---------------- Session Management ----------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    xid: String,
    cid: String,
    sn: String,
}

#[derive(Debug, Clone)]
enum SessionStatus {
    InProgress,
    Completed,
}

#[derive(Debug, Clone)]
struct SessionState {
    status: SessionStatus,
    reader_claimed: bool, // true if a request is performing the primary read
    started_at: Instant,
    completed_at: Option<Instant>,
    error_message: Option<String>, // Cache first error for repeat responses
    logical_name: Option<String>, // Original source/path for consistent short-circuit responses
}

#[derive(Debug, Clone, Copy)]
enum SessionDecision {
    StartRead,
    ShortCircuitInProgress,
    ShortCircuitCompleted,
}

struct SessionManager {
    sessions: Mutex<HashMap<SessionKey, SessionState>>, 
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    async fn decide(&self, key: SessionKey) -> SessionDecision {
        let mut sessions = self.sessions.lock().await;
        match sessions.get_mut(&key) {
            Some(state) => match state.status {
                SessionStatus::InProgress => {
                    if state.reader_claimed {
                        SessionDecision::ShortCircuitInProgress
                    } else {
                        state.reader_claimed = true;
                        SessionDecision::StartRead
                    }
                }
                SessionStatus::Completed => SessionDecision::ShortCircuitCompleted,
            },
            None => {
                sessions.insert(
                    key,
                    SessionState {
                        status: SessionStatus::InProgress,
                        reader_claimed: true,
                        started_at: Instant::now(),
                        completed_at: None,
                        error_message: None,
                        logical_name: None,
                    },
                );
                SessionDecision::StartRead
            }
        }
    }

    async fn mark_completed(&self, key: &SessionKey) {
        let mut sessions = self.sessions.lock().await;
        if let Some(state) = sessions.get_mut(key) {
            state.status = SessionStatus::Completed;
            state.completed_at = Some(Instant::now());
        }
    }

    async fn set_logical_name(&self, key: &SessionKey, name: String) {
        let mut sessions = self.sessions.lock().await;
        if let Some(state) = sessions.get_mut(key) {
            state.logical_name = Some(name);
        }
    }

    async fn set_error_message(&self, key: &SessionKey, error: String) {
        let mut sessions = self.sessions.lock().await;
        if let Some(state) = sessions.get_mut(key) {
            if state.error_message.is_none() {
                state.error_message = Some(error);
            }
        }
    }

    async fn get_session_info(&self, key: &SessionKey) -> Option<(Option<String>, Option<String>)> {
        let sessions = self.sessions.lock().await;
        sessions.get(key).map(|state| (state.logical_name.clone(), state.error_message.clone()))
    }

    async fn evict_old_sessions(&self, ttl: Duration) {
        let mut sessions = self.sessions.lock().await;
        let now = Instant::now();
        sessions.retain(|_, st| match st.status {
            SessionStatus::Completed => {
                if let Some(done) = st.completed_at {
                    now.duration_since(done) < ttl
                } else {
                    true
                }
            }
            SessionStatus::InProgress => true,
        });
    }
}

static SESSION_MANAGER: Lazy<SessionManager> = Lazy::new(SessionManager::new);

fn extract_session_key(headers: &HashMap<String, String>) -> Option<SessionKey> {
    Some(SessionKey {
        xid: headers.get("x-gp-xid")?.clone(),
        cid: headers.get("x-gp-cid")?.clone(),
        sn: headers.get("x-gp-sn")?.clone(),
    })
}

// ---------------- Configuration ----------------

// When true, repeat session responses include cached error E frame + EOF
// When false, only EOF is sent for repeat sessions
fn session_repeat_error_frame() -> bool {
    std::env::var("GPFDIST_SESSION_REPEAT_ERROR").ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(false)
}

// When true, emit F/O/L only for first packet + EOF (legacy behavior)
// When false (default), emit F/O/L before each packet (strict gpfdist)
#[cfg(feature = "compact-meta")]
const COMPACT_META_MODE: bool = true;
#[cfg(not(feature = "compact-meta"))]
const COMPACT_META_MODE: bool = false;

// ---------------- Utility ----------------

fn make_unique_table_name(source: &str, segid: Option<usize>) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time retrieval")
        .as_nanos();
    let sid = segid.unwrap_or(0);
    let sanitized: String = source
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    format!("{}_seg{}_{}", sanitized, sid, nanos)
}

// Short-circuit response for repeat sessions (in-progress or completed)
async fn send_immediate_eof_response(
    socket: &mut TcpStream,
    gp_proto: u8,
    session_key: &SessionKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: {}\r\nConnection: close\r\n\r\n",
        gp_proto
    );
    socket.write_all(response.as_bytes()).await?;
    if gp_proto == 1 {
        // Get session info for logical name and error message
        let (logical_name, error_msg) = SESSION_MANAGER.get_session_info(session_key).await
            .unwrap_or((None, None));
        
        let filename = logical_name.as_deref().unwrap_or("repeat_session");
        
        // If error cached and feature enabled, send error packet
        if session_repeat_error_frame() {
            if let Some(err_msg) = error_msg {
                // Send meta + E frame + meta + EOF for cached error
                socket.write_all(&frame_f_bytes(filename)).await?;
                socket.write_all(&frame_o_bytes(0)).await?;
                socket.write_all(&frame_l_bytes(1)).await?;
                socket.write_all(&frame_e_bytes(&err_msg)).await?;
                socket.write_all(&frame_f_bytes(filename)).await?;
                socket.write_all(&frame_o_bytes(0)).await?;
                socket.write_all(&frame_l_bytes(1)).await?;
                socket.write_all(&frame_eof_bytes()).await?;
                return Ok(());
            }
        }
        
        // Normal EOF response: meta + zero-length D
        socket.write_all(&frame_f_bytes(filename)).await?;
        socket.write_all(&frame_o_bytes(0)).await?;
        socket.write_all(&frame_l_bytes(1)).await?;
        socket.write_all(&frame_eof_bytes()).await?;
    }
    Ok(())
}

// ---------------- Server ----------------

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
            let eng = Arc::clone(&self.df_engine);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, eng).await {
                    eprintln!("connection error: {}", e);
                }
            });
        }
    }
}

// ---------------- Connection Handling ----------------

async fn handle_connection(
    mut socket: TcpStream,
    df_engine: Arc<DFEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = Vec::with_capacity(8192);
    let mut chunk = [0u8; 2048];
    while memmem::find(&buf, b"\r\n\r\n").is_none() {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            send_error(&mut socket, 400, "Header too large").await?;
            return Ok(());
        }
    }
    let split = memmem::find(&buf, b"\r\n\r\n").unwrap();
    let head_bytes = &buf[..split];
    let request = String::from_utf8_lossy(head_bytes);
    let lines: Vec<&str> = request.split("\r\n").collect();

    if lines.is_empty() {
        send_error(&mut socket, 400, "Bad Request (empty)").await?;
        return Ok(());
    }
    let parts: Vec<&str> = lines[0].split_whitespace().collect();
    if parts.len() < 2 {
        send_error(&mut socket, 400, "Bad Request (request line)").await?;
        return Ok(());
    }
    let method = parts[0];
    let path = parts[1];

    let mut headers = HashMap::new();
    for line in lines.iter().skip(1) {
        if line.is_empty() {
            break;
        }
        if let Some(p) = line.find(':') {
            headers.insert(line[..p].trim().to_lowercase(), line[p + 1..].trim().to_string());
        }
    }
    eprintln!("DEBUG headers: {:?}", headers);

    let gp_proto = headers.get("x-gp-proto").and_then(|v| v.parse::<u8>().ok());

    match method {
        "GET" => match gp_proto {
            Some(1) => {
                if path.starts_with("/df/") {
                    handle_df_route(socket, path, &headers, df_engine, 1).await?;
                } else {
                    handle_file_route(socket, path).await?;
                }
            }
            _ => send_error(&mut socket, 400, "X-GP-PROTO must be 1 for GET").await?,
        },
        "POST" => match gp_proto {
            Some(0) => send_error(&mut socket, 400, "only GET supported currently").await?,
            _ => send_error(&mut socket, 400, "X-GP-PROTO must be 0 for POST").await?,
        },
        _ => send_error(&mut socket, 400, "unsupported method").await?,
    }

    Ok(())
}

// ---------------- /df Route ----------------

async fn handle_df_route(
    socket: TcpStream,
    path: &str,
    headers: &HashMap<String, String>,
    df_engine: Arc<DFEngine>,
    gp_proto: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let path_without_query = path.split('?').next().unwrap_or(path);
    let parts: Vec<&str> = path_without_query.split('/').collect();
    let mut raw_socket = socket;

    if parts.len() < 3 {
        send_error(&mut raw_socket, 400, "Invalid /df/ route").await?;
        return Ok(());
    }

    let source = parts[2];
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

    let query_map = parse_query_map(path);
    let mut uri = query_map.get("path").cloned();
    if uri.is_none() && parts.len() > 3 {
        let rest = parts[3..].join("/");
        if !rest.is_empty() {
            uri = Some(percent_decode(&rest));
        }
    }

    let files_str = query_map.get("files").cloned();
    if uri.is_none() && files_str.is_none() {
        send_error(&mut raw_socket, 400, "Missing 'path' or 'files' parameter").await?;
        return Ok(());
    }

    let file_list = files_str.map(|s| {
        percent_decode(&s)
            .split(',')
            .map(|x| x.trim().to_string())
            .collect::<Vec<_>>()
    });

    let projection = query_map.get("columns").map(|s| {
        percent_decode(s)
            .split(',')
            .map(|x| x.trim().to_string())
            .collect::<Vec<_>>()
    });

    let filter = query_map.get("filter").map(|s| percent_decode(s));
    let limit = query_map.get("limit").and_then(|s| s.parse::<usize>().ok());

    let segment_id = headers
        .get("x-gp-segment-id")
        .and_then(|s| s.parse::<usize>().ok());
    let segment_count = headers
        .get("x-gp-segment-count")
        .and_then(|s| s.parse::<usize>().ok());

    let session_key = extract_session_key(headers);

    let use_session_caching = match table_type {
        #[cfg(feature = "delta")]
        TableType::Delta => file_list.is_none(),
        #[cfg(feature = "iceberg")]
        TableType::Iceberg => file_list.is_none(),
        TableType::Parquet => false,
    };

    if let Some(key) = &session_key {
        if use_session_caching {
            SESSION_MANAGER
                .evict_old_sessions(Duration::from_secs(300))
                .await;

            match SESSION_MANAGER.decide(key.clone()).await {
                SessionDecision::StartRead => {
                    eprintln!("Session {:?} starting primary read", key);
                }
                SessionDecision::ShortCircuitInProgress => {
                    eprintln!(
                        "Session {:?} already has active reader (in-progress), short-circuit",
                        key
                    );
                    send_immediate_eof_response(&mut raw_socket, gp_proto, key).await?;
                    return Ok(());
                }
                SessionDecision::ShortCircuitCompleted => {
                    eprintln!("Session {:?} completed previously, short-circuit", key);
                    send_immediate_eof_response(&mut raw_socket, gp_proto, key).await?;
                    return Ok(());
                }
            }
        }
    }

    let table_name = if uri.is_some() && file_list.is_none() {
        Some(make_unique_table_name(source, segment_id))
    } else {
        None
    };

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

    match df_engine.execute_csv_batches(request).await {
        Ok(mut stream) => {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: {}\r\nConnection: close\r\n\r\n",
                gp_proto
            );
            raw_socket.write_all(response.as_bytes()).await?;

            let mut writer = BufWriter::with_capacity(64 * 1024, raw_socket);

            if gp_proto == 1 {
                let mut offset: u64 = 0;
                let mut line_no: u64 = 1; // first line number (1-indexed)
                let mut last_chunk_ended_with_newline = true; // track partial line state
                
                // Set logical name for session short-circuit responses
                if let Some(key) = &session_key {
                    if use_session_caching {
                        SESSION_MANAGER.set_logical_name(key, source.to_string()).await;
                    }
                }

                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(csv_bytes) => {
                            // Write data packet with current offset and line_no
                            write_data_packet(&mut writer, source, offset, line_no, &csv_bytes).await?;

                            // Update offset: only count CSV payload bytes
                            offset += csv_bytes.len() as u64;
                            
                            // Update line_no: only count completed lines
                            let newline_count = bytecount::count(&csv_bytes, b'\n') as u64;
                            line_no += newline_count;
                            
                            // Track if this chunk ends with newline for proper line counting
                            last_chunk_ended_with_newline = csv_bytes.last() == Some(&b'\n');
                        }
                        Err(e) => {
                            // Error packet: send meta + E + meta + EOF
                            let err_msg = format!("ERROR: {}", e);
                            
                            // Cache error message in session
                            if let Some(key) = &session_key {
                                if use_session_caching {
                                    SESSION_MANAGER.set_error_message(key, err_msg.clone()).await;
                                }
                            }
                            
                            write_error_packet(&mut writer, source, offset, line_no, &err_msg).await?;
                            write_eof_packet(&mut writer, source, offset, line_no).await?;
                            writer.flush().await?;
                            if let Some(key) = &session_key { if use_session_caching { SESSION_MANAGER.mark_completed(key).await; } }
                            return Ok(());
                        }
                    }
                }
                
                // At EOF: if last chunk didn't end with newline and we sent data, count the final partial line
                if !last_chunk_ended_with_newline && offset > 0 {
                    line_no += 1;
                }
                
                // Final EOF packet includes meta frames
                write_eof_packet(&mut writer, source, offset, line_no).await?;
                writer.flush().await?;
            } else {
                while let Some(chunk_result) = stream.next().await {
                    if let Ok(csv_bytes) = chunk_result {
                        writer.write_all(&csv_bytes).await?;
                    }
                }
                writer.flush().await?;
            }

            if let Some(key) = &session_key { if use_session_caching { SESSION_MANAGER.mark_completed(key).await; } }
        }
        Err(e) => {
            send_error(&mut raw_socket, 500, &format!("Query execution failed: {}", e)).await?;
            if let Some(key) = &session_key { if use_session_caching { SESSION_MANAGER.mark_completed(key).await; } }
        }
    }

    Ok(())
}

// ---------------- Fallback File Route ----------------

async fn handle_file_route(
    mut socket: TcpStream,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (file_path, query_str) = if let Some(pos) = path.find('?') {
        (&path[..pos], Some(&path[pos + 1..]))
    } else {
        (path, None)
    };

    let decoded_path = percent_decode(file_path);

    if decoded_path.contains("..") {
        send_error(&mut socket, 400, "Path traversal not allowed").await?;
        return Ok(());
    }

    let lines_limit = if let Some(query) = query_str {
        let query_map = parse_query_map(&format!("?{}", query));
        query_map.get("lines").and_then(|s| s.parse::<usize>().ok())
    } else {
        None
    };

    let resolved_path = std::env::current_dir()?.join(&decoded_path.trim_start_matches('/'));

    let mut file = match File::open(&resolved_path).await {
        Ok(f) => f,
        Err(e) => {
            let err_msg = format!("Failed to read file: {}, error {}", resolved_path.display(), e);
            eprintln!("{}", err_msg);
            let response = "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: 1\r\nConnection: close\r\n\r\n";
            socket.write_all(response.as_bytes()).await?;
            
            let mut writer = BufWriter::with_capacity(64 * 1024, socket);
            // Error packet: meta + E + meta + EOF
            write_error_packet(&mut writer, &decoded_path, 0, 1, &err_msg).await?;
            write_eof_packet(&mut writer, &decoded_path, 0, 1).await?;
            writer.flush().await?;
            return Ok(());
        }
    };

    let response = "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: 1\r\nConnection: close\r\n\r\n";
    socket.write_all(response.as_bytes()).await?;

    let mut writer = BufWriter::with_capacity(64 * 1024, socket);

    let mut offset: u64 = 0;
    let mut line_no: u64 = 1;
    let mut buf = vec![0u8; 32 * 1024];
    let mut lines_sent = 0;
    let mut stop_streaming = false;
    let mut last_chunk_ended_with_newline = true;

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 { break; }
        let mut chunk = &buf[..n];

        if let Some(limit) = lines_limit {
            let newlines_in_chunk = bytecount::count(chunk, b'\n');
            if lines_sent + newlines_in_chunk >= limit {
                let needed = limit - lines_sent;
                let mut pos = 0; let mut found = 0;
                for (i, &b) in chunk.iter().enumerate() {
                    if b == b'\n' { found += 1; if found == needed { pos = i + 1; break; } }
                }
                if pos > 0 { chunk = &chunk[..pos]; }
                stop_streaming = true;
            }
            lines_sent += newlines_in_chunk;
        }

        // Write data packet with meta frames
        write_data_packet(&mut writer, &decoded_path, offset, line_no, chunk).await?;

        // Update offset: only count CSV payload bytes
        offset += chunk.len() as u64;
        
        // Update line_no: only count completed lines
        let newline_count = bytecount::count(chunk, b'\n') as u64;
        line_no += newline_count;
        
        // Track if chunk ends with newline
        last_chunk_ended_with_newline = chunk.last() == Some(&b'\n');

        if stop_streaming { break; }
    }

    // At EOF: if last chunk didn't end with newline and we sent data, count the final partial line
    if !last_chunk_ended_with_newline && offset > 0 {
        line_no += 1;
    }

    // EOF packet with meta frames
    write_eof_packet(&mut writer, &decoded_path, offset, line_no).await?;

    writer.flush().await?;
    Ok(())
}

// ---------------- Framing Helpers ----------------

fn frame_hdr_bytes(letter: u8, len: u32) -> [u8; 5] {
    let mut b = [0u8; 5];
    b[0] = letter;
    b[1..5].copy_from_slice(&len.to_be_bytes());
    b
}

fn frame_f_bytes(filename: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + filename.len());
    buf.extend_from_slice(&frame_hdr_bytes(b'F', filename.len() as u32));
    buf.extend_from_slice(filename.as_bytes());
    buf
}

fn frame_o_bytes(offset: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + 8);
    buf.extend_from_slice(&frame_hdr_bytes(b'O', 8));
    buf.extend_from_slice(&offset.to_be_bytes());
    buf
}

fn frame_l_bytes(line_no: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + 8);
    buf.extend_from_slice(&frame_hdr_bytes(b'L', 8));
    buf.extend_from_slice(&line_no.to_be_bytes());
    buf
}

fn frame_e_bytes(msg: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + msg.len());
    buf.extend_from_slice(&frame_hdr_bytes(b'E', msg.len() as u32));
    buf.extend_from_slice(msg.as_bytes());
    buf
}

fn frame_eof_bytes() -> [u8; 5] { frame_hdr_bytes(b'D', 0) }

// ---------------- High-Level Framing Helpers ----------------

/// Write F/O/L meta frames to the writer
async fn write_meta<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    filename: &str,
    offset: u64,
    line_no: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    writer.write_all(&frame_f_bytes(filename)).await?;
    writer.write_all(&frame_o_bytes(offset)).await?;
    writer.write_all(&frame_l_bytes(line_no)).await?;
    Ok(())
}

/// Write a complete data packet: F/O/L + D frame with CSV bytes
async fn write_data_packet<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    filename: &str,
    offset: u64,
    line_no: u64,
    csv_bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    write_meta(writer, filename, offset, line_no).await?;
    let header = frame_hdr_bytes(b'D', csv_bytes.len() as u32);
    writer.write_all(&header).await?;
    writer.write_all(csv_bytes).await?;
    Ok(())
}

/// Write an error packet: F/O/L + E frame
async fn write_error_packet<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    filename: &str,
    offset: u64,
    line_no: u64,
    err_msg: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    write_meta(writer, filename, offset, line_no).await?;
    writer.write_all(&frame_e_bytes(err_msg)).await?;
    Ok(())
}

/// Write an EOF packet: F/O/L + zero-length D frame
async fn write_eof_packet<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    filename: &str,
    offset: u64,
    line_no: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    write_meta(writer, filename, offset, line_no).await?;
    writer.write_all(&frame_eof_bytes()).await?;
    Ok(())
}

// ---------------- Error Response ----------------

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
        "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        status_text,
        message.len(),
        message
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}