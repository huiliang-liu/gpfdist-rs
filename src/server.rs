use crate::df_engine::{DFEngine, DFRequest, TableType};
use crate::util::{parse_query_map, percent_decode};
use bytes::BytesMut;
use futures::StreamExt;
use once_cell::sync::Lazy;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Notify};

use memchr::memmem;

// ---------------- Session Management (Sequential Slice Model) ----------------

/// Unique identifier for a gpfdist session (X-GP-XID, X-GP-CID, X-GP-SN)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    xid: String,
    cid: String,
    sn: String,
}

/// Phase of the session lifecycle
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum SessionPhase {
    /// Session is actively reading data
    Reading,
    /// Session has successfully completed (all data consumed)
    Completed,
    /// Session encountered an error (error message stored for replay)
    Error(String),
}

/// A slice of data to be sent to a segment
#[derive(Debug, Clone)]
struct DataSlice {
    /// The CSV data bytes
    data: Vec<u8>,
    /// Starting byte offset for this slice
    offset: u64,
    /// Starting line number for this slice
    line_number: u64,
}

/// Error frames to replay on error
#[derive(Debug, Clone)]
struct CachedErrorFrames {
    error_message: String,
    offset: u64,
    line_number: u64,
}

/// A waiting segment connection
struct SegmentSink {
    /// Sender to deliver slices to this segment
    tx: mpsc::Sender<SliceResult>,
    /// Target bytes threshold for this segment's slice (0 = no limit)
    target_bytes: usize,
    /// Target lines threshold for this segment's slice (0 = no limit)
    target_lines: usize,
    /// Bytes already assigned to this segment's current slice
    bytes_assigned: usize,
    /// Lines already assigned to this segment's current slice
    lines_assigned: usize,
    /// Whether this segment has received all its data
    finished: bool,
}

/// Result sent to a segment sink
#[derive(Debug, Clone)]
enum SliceResult {
    /// Data slice to send
    Data(DataSlice),
    /// End of data for this segment
    Eof { offset: u64, line_number: u64 },
    /// Error occurred
    Error(CachedErrorFrames),
}

/// Shared state for a session
struct SessionShared {
    /// Current phase of the session
    phase: SessionPhase,
    /// Next byte offset (updated as data is read)
    next_offset: u64,
    /// Next line number (updated as data is read)
    next_line: u64,
    /// Whether the last batch ended with a newline
    #[allow(dead_code)]
    last_batch_ended_with_newline: bool,
    /// Queue of waiting segment sinks
    pending_queue: VecDeque<SegmentSink>,
    /// Index of the currently active sink receiving data
    active_sink_index: Option<usize>,
    /// Cached error frames for replay
    cached_error_frames: Option<CachedErrorFrames>,
    /// Logical filename for F frames (may be used for future enhancements)
    #[allow(dead_code)]
    logical_name: String,
    /// Timestamp when session completed (for TTL eviction)
    completed_at: Option<Instant>,
    /// Notify when new sinks are added
    sink_notify: Arc<Notify>,
    /// Whether the reader task has been started
    reader_started: bool,
}

impl SessionShared {
    fn new(logical_name: String) -> Self {
        Self {
            phase: SessionPhase::Reading,
            next_offset: 0,
            next_line: 1,
            last_batch_ended_with_newline: true,
            pending_queue: VecDeque::new(),
            active_sink_index: None,
            cached_error_frames: None,
            logical_name,
            completed_at: None,
            sink_notify: Arc::new(Notify::new()),
            reader_started: false,
        }
    }
}

/// Manager for all sessions
struct SessionManager {
    sessions: Mutex<HashMap<SessionKey, Arc<Mutex<SessionShared>>>>,
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a session, returns (session, is_new)
    async fn get_or_create(
        &self,
        key: SessionKey,
        logical_name: String,
    ) -> (Arc<Mutex<SessionShared>>, bool) {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get(&key) {
            (Arc::clone(session), false)
        } else {
            let session = Arc::new(Mutex::new(SessionShared::new(logical_name)));
            sessions.insert(key, Arc::clone(&session));
            (session, true)
        }
    }

    /// Evict old completed sessions
    async fn evict_old_sessions(&self, ttl: Duration) {
        let mut sessions = self.sessions.lock().await;
        let now = Instant::now();
        
        let mut keys_to_remove = Vec::new();
        for (key, session) in sessions.iter() {
            let guard = session.lock().await;
            if let Some(completed_at) = guard.completed_at {
                if now.duration_since(completed_at) >= ttl {
                    keys_to_remove.push(key.clone());
                }
            }
        }
        
        for key in keys_to_remove {
            sessions.remove(&key);
        }
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

/// Get slice threshold configuration from environment
fn get_slice_thresholds() -> (usize, usize) {
    // Prefer line-based threshold if GPFDIST_SEGMENT_TARGET_LINES is set
    let target_lines = std::env::var("GPFDIST_SEGMENT_TARGET_LINES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    
    // Fallback to bytes threshold (default 1 MB)
    let target_bytes = std::env::var("GPFDIST_SEGMENT_TARGET_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024 * 1024);
    
    // If lines threshold is set, use it; otherwise use bytes
    if target_lines > 0 {
        (0, target_lines)
    } else {
        (target_bytes, 0)
    }
}

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

    // Determine if we should use session-based slice distribution
    // Session caching is used for Delta/Iceberg when using path (not explicit file list)
    let use_session_slicing = match table_type {
        #[cfg(feature = "delta")]
        TableType::Delta => file_list.is_none() && session_key.is_some(),
        #[cfg(feature = "iceberg")]
        TableType::Iceberg => file_list.is_none() && session_key.is_some(),
        TableType::Parquet => false, // Parquet uses file-based segmentation, not session slicing
    };

    if use_session_slicing {
        // Use session-based slice distribution (Sequential Slice Model)
        let key = session_key.as_ref().unwrap().clone();
        handle_df_route_with_session(
            raw_socket,
            df_engine,
            &key,
            source.to_string(),
            table_type,
            uri.unwrap_or_default(),
            file_list,
            projection,
            filter,
            limit,
            segment_id,
            segment_count,
            gp_proto,
        )
        .await
    } else {
        // Use traditional per-request execution (no session slicing)
        handle_df_route_direct(
            raw_socket,
            df_engine,
            source,
            table_type,
            uri.unwrap_or_default(),
            file_list,
            projection,
            filter,
            limit,
            segment_id,
            segment_count,
            gp_proto,
        )
        .await
    }
}

/// Handle /df route with session-based slice distribution (Sequential Slice Model)
#[allow(clippy::too_many_arguments)]
async fn handle_df_route_with_session(
    mut socket: TcpStream,
    df_engine: Arc<DFEngine>,
    session_key: &SessionKey,
    source: String,
    table_type: TableType,
    uri: String,
    file_list: Option<Vec<String>>,
    projection: Option<Vec<String>>,
    filter: Option<String>,
    limit: Option<usize>,
    segment_id: Option<usize>,
    _segment_count: Option<usize>,
    gp_proto: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    // Evict old sessions periodically
    SESSION_MANAGER
        .evict_old_sessions(Duration::from_secs(300))
        .await;

    // Get or create the session
    let (session, is_new) = SESSION_MANAGER
        .get_or_create(session_key.clone(), source.clone())
        .await;

    // Get slice thresholds from environment
    let (target_bytes, target_lines) = get_slice_thresholds();

    // Create a channel for receiving slices
    let (tx, mut rx) = mpsc::channel::<SliceResult>(16);

    // Register this segment as a waiting sink
    {
        let mut guard = session.lock().await;
        
        // Check if session is already in a terminal state
        match &guard.phase {
            SessionPhase::Completed => {
                // Session already completed - send immediate EOF
                drop(guard);
                send_eof_response(&mut socket, &source, 0, 1, gp_proto).await?;
                return Ok(());
            }
            SessionPhase::Error(_) => {
                // Session had an error - replay error frames
                if let Some(ref cached) = guard.cached_error_frames {
                    let cached = cached.clone();
                    drop(guard);
                    send_error_response(&mut socket, &source, &cached, gp_proto).await?;
                    return Ok(());
                }
            }
            SessionPhase::Reading => {
                // Session is still reading - add ourselves to the queue
            }
        }

        let sink = SegmentSink {
            tx,
            target_bytes,
            target_lines,
            bytes_assigned: 0,
            lines_assigned: 0,
            finished: false,
        };
        guard.pending_queue.push_back(sink);
        
        // Notify the reader task that a new sink is available
        guard.sink_notify.notify_one();

        // If this is a new session, start the reader task
        if is_new && !guard.reader_started {
            guard.reader_started = true;
            let session_clone = Arc::clone(&session);
            let df_engine_clone = Arc::clone(&df_engine);
            
            // Build the request for the reader task
            let table_name = Some(make_unique_table_name(&source, segment_id));
            let request = DFRequest {
                table_type,
                uri,
                file_list,
                projection,
                filter,
                limit,
                // Reader task performs a full table scan (no segment-level query filtering).
                // Slice distribution to segments is handled at the session layer, not at query level.
                segment_id: None,
                segment_count: None,
                gp_proto,
                table_name,
            };

            // Spawn the background reader task
            tokio::spawn(async move {
                run_session_reader(session_clone, df_engine_clone, request).await;
            });
        }
    }

    // Send HTTP response header
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: {}\r\nConnection: close\r\n\r\n",
        gp_proto
    );
    socket.write_all(response.as_bytes()).await?;

    let mut writer = BufWriter::with_capacity(64 * 1024, socket);

    // Receive slices from the reader task and send to client
    while let Some(slice_result) = rx.recv().await {
        match slice_result {
            SliceResult::Data(slice) => {
                if gp_proto == 1 {
                    // Write F/O/L/D frames
                    writer.write_all(&frame_f_bytes(&source)).await?;
                    writer.write_all(&frame_o_bytes(slice.offset)).await?;
                    writer.write_all(&frame_l_bytes(slice.line_number)).await?;
                    let header = frame_hdr_bytes(b'D', slice.data.len() as u32);
                    writer.write_all(&header).await?;
                    writer.write_all(&slice.data).await?;
                } else {
                    writer.write_all(&slice.data).await?;
                }
            }
            SliceResult::Eof { offset, line_number } => {
                if gp_proto == 1 {
                    // Write F/O/L + EOF (D with length 0)
                    eprintln!("Sending EOF frame: F/O/L + EOF at offset {}, line {}", offset, line_number);
                    writer.write_all(&frame_f_bytes(&source)).await?;
                    writer.write_all(&frame_o_bytes(offset)).await?;
                    writer.write_all(&frame_l_bytes(line_number)).await?;
                    writer.write_all(&frame_eof_bytes()).await?;
                }
                break;
            }
            SliceResult::Error(cached) => {
                if gp_proto == 1 {
                    // Write F/O/L + E + F/O/L + EOF
                    writer.write_all(&frame_f_bytes(&source)).await?;
                    writer.write_all(&frame_o_bytes(cached.offset)).await?;
                    writer.write_all(&frame_l_bytes(cached.line_number)).await?;
                    let err_msg = format!("ERROR: {}", cached.error_message);
                    writer.write_all(&frame_e_bytes(&err_msg)).await?;
                    writer.write_all(&frame_f_bytes(&source)).await?;
                    writer.write_all(&frame_o_bytes(cached.offset)).await?;
                    writer.write_all(&frame_l_bytes(cached.line_number)).await?;
                    writer.write_all(&frame_eof_bytes()).await?;
                }
                break;
            }
        }
    }

    writer.flush().await?;
    Ok(())
}

/// Background reader task that pulls data from DataFusion and distributes to waiting segments
async fn run_session_reader(
    session: Arc<Mutex<SessionShared>>,
    df_engine: Arc<DFEngine>,
    request: DFRequest,
) {
    // Execute the query
    let stream_result = df_engine.execute_csv_batches(request).await;

    match stream_result {
        Ok(mut stream) => {
            // Process each batch from the stream
            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(csv_bytes) => {
                        if csv_bytes.is_empty() {
                            continue;
                        }

                        // Calculate line count for this batch
                        let newline_count = bytecount::count(&csv_bytes, b'\n') as u64;
                        let ends_with_newline = csv_bytes.last() == Some(&b'\n');
                        let bytes_len = csv_bytes.len() as u64;

                        // Get current offset and line, then create the slice
                        let mut guard = session.lock().await;
                        let slice = DataSlice {
                            data: csv_bytes.clone(),
                            offset: guard.next_offset,
                            line_number: guard.next_line,
                        };

                        // Update the session state
                        let extra_line = if !ends_with_newline { 1 } else { 0 };
                        guard.next_offset += bytes_len;
                        guard.next_line += newline_count + extra_line;
                        guard.last_batch_ended_with_newline = ends_with_newline;

                        // Distribute the slice to the active sink or wait for one
                        if !distribute_slice_to_sink(&mut guard, SliceResult::Data(slice)).await {
                            // Distribution failed - can happen if all current sinks are finished
                            // or disconnected. Wait for a new sink to register before retrying.
                            let notify = Arc::clone(&guard.sink_notify);
                            let saved_offset = guard.next_offset - bytes_len;
                            let saved_line = guard.next_line - newline_count - extra_line;
                            drop(guard);
                            notify.notified().await;
                            
                            // Retry distribution with a new slice after a sink becomes available
                            let mut guard = session.lock().await;
                            let slice = DataSlice {
                                data: csv_bytes,
                                offset: saved_offset,
                                line_number: saved_line,
                            };
                            let _ = distribute_slice_to_sink(&mut guard, SliceResult::Data(slice)).await;
                        }
                    }
                    Err(e) => {
                        // Error occurred - cache error and notify all waiting sinks
                        let mut guard = session.lock().await;
                        let cached = CachedErrorFrames {
                            error_message: e.clone(),
                            offset: guard.next_offset,
                            line_number: guard.next_line,
                        };
                        guard.cached_error_frames = Some(cached.clone());
                        guard.phase = SessionPhase::Error(e);
                        
                        // Send error to all waiting sinks
                        for sink in guard.pending_queue.iter_mut() {
                            if !sink.finished {
                                let _ = sink.tx.send(SliceResult::Error(cached.clone())).await;
                                sink.finished = true;
                            }
                        }
                        guard.completed_at = Some(Instant::now());
                        return;
                    }
                }
            }

            // All data consumed - send EOF to all remaining sinks
            let mut guard = session.lock().await;
            guard.phase = SessionPhase::Completed;
            let final_offset = guard.next_offset;
            let final_line = guard.next_line;
            
            // Send EOF to all waiting sinks
            for sink in guard.pending_queue.iter_mut() {
                if !sink.finished {
                    let _ = sink.tx.send(SliceResult::Eof {
                        offset: final_offset,
                        line_number: final_line,
                    }).await;
                    sink.finished = true;
                }
            }
            guard.completed_at = Some(Instant::now());
        }
        Err(e) => {
            // Initial query execution failed
            let mut guard = session.lock().await;
            let cached = CachedErrorFrames {
                error_message: e.clone(),
                offset: 0,
                line_number: 1,
            };
            guard.cached_error_frames = Some(cached.clone());
            guard.phase = SessionPhase::Error(e);
            
            // Send error to all waiting sinks
            for sink in guard.pending_queue.iter_mut() {
                if !sink.finished {
                    let _ = sink.tx.send(SliceResult::Error(cached.clone())).await;
                    sink.finished = true;
                }
            }
            guard.completed_at = Some(Instant::now());
        }
    }
}

/// Distribute a slice to the currently active sink or find a new one
async fn distribute_slice_to_sink(
    guard: &mut tokio::sync::MutexGuard<'_, SessionShared>,
    slice: SliceResult,
) -> bool {
    // Find or activate a sink
    if guard.active_sink_index.is_none() {
        // Find the first non-finished sink
        for (i, sink) in guard.pending_queue.iter().enumerate() {
            if !sink.finished {
                guard.active_sink_index = Some(i);
                break;
            }
        }
    }

    if let Some(idx) = guard.active_sink_index {
        if idx < guard.pending_queue.len() {
            // Extract needed information from the sink up front
            let sink_info = {
                let sink = &guard.pending_queue[idx];
                (sink.finished, sink.target_bytes, sink.target_lines, sink.tx.clone())
            };
            let (sink_finished, _target_bytes, _target_lines, tx) = sink_info;
            
            if !sink_finished {
                // Calculate slice metrics
                let slice_bytes = match &slice {
                    SliceResult::Data(d) => d.data.len(),
                    _ => 0,
                };
                let slice_lines = match &slice {
                    SliceResult::Data(d) => bytecount::count(&d.data, b'\n'),
                    _ => 0,
                };

                // Send the slice using the cloned sender
                if tx.send(slice).await.is_ok() {
                    // Update sink state after successful send
                    let sink = &mut guard.pending_queue[idx];
                    sink.bytes_assigned += slice_bytes;
                    sink.lines_assigned += slice_lines;
                    return true;
                } else {
                    // Sink disconnected - mark as finished
                    guard.pending_queue[idx].finished = true;
                    guard.active_sink_index = None;
                }
            }
        }
    }

    false
}

/// Send an immediate EOF response (for completed sessions)
async fn send_eof_response(
    socket: &mut TcpStream,
    source: &str,
    offset: u64,
    line_number: u64,
    gp_proto: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: {}\r\nConnection: close\r\n\r\n",
        gp_proto
    );
    socket.write_all(response.as_bytes()).await?;
    
    if gp_proto == 1 {
        socket.write_all(&frame_f_bytes(source)).await?;
        socket.write_all(&frame_o_bytes(offset)).await?;
        socket.write_all(&frame_l_bytes(line_number)).await?;
        socket.write_all(&frame_eof_bytes()).await?;
    }
    Ok(())
}

/// Send an error response (for sessions that encountered an error)
async fn send_error_response(
    socket: &mut TcpStream,
    source: &str,
    cached: &CachedErrorFrames,
    gp_proto: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-GP-PROTO: {}\r\nConnection: close\r\n\r\n",
        gp_proto
    );
    socket.write_all(response.as_bytes()).await?;
    
    if gp_proto == 1 {
        // F/O/L + E + F/O/L + EOF
        socket.write_all(&frame_f_bytes(source)).await?;
        socket.write_all(&frame_o_bytes(cached.offset)).await?;
        socket.write_all(&frame_l_bytes(cached.line_number)).await?;
        let err_msg = format!("ERROR: {}", cached.error_message);
        socket.write_all(&frame_e_bytes(&err_msg)).await?;
        socket.write_all(&frame_f_bytes(source)).await?;
        socket.write_all(&frame_o_bytes(cached.offset)).await?;
        socket.write_all(&frame_l_bytes(cached.line_number)).await?;
        socket.write_all(&frame_eof_bytes()).await?;
    }
    Ok(())
}

/// Handle /df route with direct execution (no session slicing)
#[allow(clippy::too_many_arguments)]
async fn handle_df_route_direct(
    mut socket: TcpStream,
    df_engine: Arc<DFEngine>,
    source: &str,
    table_type: TableType,
    uri: String,
    file_list: Option<Vec<String>>,
    projection: Option<Vec<String>>,
    filter: Option<String>,
    limit: Option<usize>,
    segment_id: Option<usize>,
    segment_count: Option<usize>,
    gp_proto: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_name = if file_list.is_none() {
        Some(make_unique_table_name(source, segment_id))
    } else {
        None
    };

    let request = DFRequest {
        table_type,
        uri,
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
            socket.write_all(response.as_bytes()).await?;

            let mut writer = BufWriter::with_capacity(64 * 1024, socket);

            if gp_proto == 1 {
                let mut offset: u64 = 0;
                let mut line_no: u64 = 1;

                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(csv_bytes) => {
                            // Emit F/O/L/D frames
                            writer.write_all(&frame_f_bytes(source)).await?;
                            writer.write_all(&frame_o_bytes(offset)).await?;
                            writer.write_all(&frame_l_bytes(line_no)).await?;

                            let header = frame_hdr_bytes(b'D', csv_bytes.len() as u32);
                            writer.write_all(&header).await?;
                            writer.write_all(&csv_bytes).await?;

                            // Update offset & line_no for next packet
                            offset += csv_bytes.len() as u64;
                            let newline_count = bytecount::count(&csv_bytes, b'\n') as u64;
                            let extra_line = if !csv_bytes.is_empty() && *csv_bytes.last().unwrap() != b'\n' { 1 } else { 0 };
                            line_no += newline_count + extra_line;
                        }
                        Err(e) => {
                            // Error packet: F/O/L + E + F/O/L + EOF
                            writer.write_all(&frame_f_bytes(source)).await?;
                            writer.write_all(&frame_o_bytes(offset)).await?;
                            writer.write_all(&frame_l_bytes(line_no)).await?;
                            let err_msg = format!("ERROR: {}", e);
                            writer.write_all(&frame_e_bytes(&err_msg)).await?;
                            writer.write_all(&frame_f_bytes(source)).await?;
                            writer.write_all(&frame_o_bytes(offset)).await?;
                            writer.write_all(&frame_l_bytes(line_no)).await?;
                            writer.write_all(&frame_eof_bytes()).await?;
                            writer.flush().await?;
                            return Ok(());
                        }
                    }
                }
                // Final EOF packet
                println!("Sending final EOF frame: F/O/L + EOF at offset {}, line {}", offset, line_no);

                writer.write_all(&frame_f_bytes(source)).await?;
                writer.write_all(&frame_o_bytes(offset)).await?;
                writer.write_all(&frame_l_bytes(line_no)).await?;
                writer.write_all(&frame_eof_bytes()).await?;
                writer.flush().await?;
            } else {
                while let Some(chunk_result) = stream.next().await {
                    if let Ok(csv_bytes) = chunk_result {
                        writer.write_all(&csv_bytes).await?;
                    }
                }
                writer.flush().await?;
            }
        }
        Err(e) => {
            send_error(&mut socket, 500, &format!("Query execution failed: {}", e)).await?;
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
            // Error packet
            socket.write_all(&frame_f_bytes(&decoded_path)).await?;
            socket.write_all(&frame_o_bytes(0)).await?;
            socket.write_all(&frame_l_bytes(1)).await?;
            socket.write_all(&frame_e_bytes(&err_msg)).await?;
            // EOF packet
            socket.write_all(&frame_f_bytes(&decoded_path)).await?;
            socket.write_all(&frame_o_bytes(0)).await?;
            socket.write_all(&frame_l_bytes(1)).await?;
            socket.write_all(&frame_eof_bytes()).await?;
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

        // Meta frames before each D frame
        writer.write_all(&frame_f_bytes(&decoded_path)).await?;
        writer.write_all(&frame_o_bytes(offset)).await?;
        writer.write_all(&frame_l_bytes(line_no)).await?;

        let header = frame_hdr_bytes(b'D', chunk.len() as u32);
        writer.write_all(&header).await?;
        writer.write_all(chunk).await?;

        offset += chunk.len() as u64;
        let newline_count = bytecount::count(chunk, b'\n') as u64;
        let extra_line = if !chunk.is_empty() && chunk[chunk.len()-1] != b'\n' { 1 } else { 0 };
        line_no += newline_count + extra_line;

        if stop_streaming { break; }
    }

    // EOF packet with meta frames
    writer.write_all(&frame_f_bytes(&decoded_path)).await?;
    writer.write_all(&frame_o_bytes(offset)).await?;
    writer.write_all(&frame_l_bytes(line_no)).await?;
    writer.write_all(&frame_eof_bytes()).await?;

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
