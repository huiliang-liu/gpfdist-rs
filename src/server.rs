use crate::df_engine::{DFEngine, DFRequest, TableType};
use crate::util::{parse_query_map, percent_decode};
use futures::StreamExt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use bytes::BytesMut;

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
    let gp_proto = headers
        .get("x-gp-proto")
        .and_then(|s| s.parse::<u8>().ok());

    // Enforce X-GP-PROTO protocol mapping rules
    match method {
        "GET" => {
            // For GET requests, X-GP-PROTO MUST be 1
            match gp_proto {
                Some(1) => {
                    // Valid GET with gp_proto=1
                    if path.starts_with("/df/") {
                        handle_df_route(&mut socket, path, &headers, df_engine).await?;
                    } else {
                        // Fallback file serving for non-/df/ paths
                        handle_file_route(&mut socket, path).await?;
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
    socket: &mut TcpStream,
    path: &str,
    headers: &std::collections::HashMap<String, String>,
    df_engine: Arc<DFEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract source type from path: /df/{source}
    let path_without_query = path.split('?').next().unwrap_or(path);
    let parts: Vec<&str> = path_without_query.split('/').collect();
    
    if parts.len() < 3 {
        send_error(socket, 400, "Invalid /df/ route").await?;
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
            send_error(socket, 400, "Delta feature not enabled").await?;
            return Ok(());
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => TableType::Iceberg,
        #[cfg(not(feature = "iceberg"))]
        "iceberg" => {
            send_error(socket, 400, "Iceberg feature not enabled").await?;
            return Ok(());
        }
        _ => {
            send_error(socket, 400, "Unknown source type").await?;
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
        send_error(socket, 400, "Missing 'path' or 'files' parameter").await?;
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
        send_error(socket, 400, "Invalid X-GP-PROTO (must be 0 or 1)").await?;
        return Ok(());
    }

    let segment_id = headers
        .get("x-gp-segment-id")
        .and_then(|s| s.parse::<usize>().ok());

    let segment_count = headers
        .get("x-gp-segment-count")
        .and_then(|s| s.parse::<usize>().ok());

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
            socket.write_all(response.as_bytes()).await?;

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
                                // F frame: source label (use source type as label)
                                socket.write_all(&frame_f(&source)).await?;
                                
                                // O frame: offset = 0
                                socket.write_all(&frame_o(0)).await?;
                                
                                // L frame: line_no = 1
                                socket.write_all(&frame_l(1)).await?;
                                
                                first_batch = false;
                            }

                            // Count lines in this batch
                            let newline_count = bytecount::count(&csv_bytes, b'\n');
                            let rows_in_batch = if csv_bytes.is_empty() || csv_bytes[csv_bytes.len() - 1] == b'\n' {
                                newline_count as u64
                            } else {
                                // Last line doesn't end with newline, add 1
                                newline_count as u64 + 1
                            };

                            // Emit D frame with CSV data
                            socket.write_all(&frame_d(&csv_bytes)).await?;

                            // Update state
                            _offset += csv_bytes.len() as u64;
                            _line_no += rows_in_batch;
                        }
                        Err(e) => {
                            eprintln!("Stream error: {}", e);
                            // Send error frame + EOF
                            let err_msg = format!("ERROR: {}", e);
                            socket.write_all(&frame_e(&err_msg)).await?;
                            socket.write_all(&frame_eof()).await?;
                            return Ok(());
                        }
                    }
                }

                // After stream ends: emit EOF (zero-length D frame)
                socket.write_all(&frame_eof()).await?;
            } else {
                // Protocol 0: raw CSV only (no framing)
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            socket.write_all(&chunk).await?;
                        }
                        Err(e) => {
                            eprintln!("Stream error: {}", e);
                            break;
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to execute query: {}", e);
            send_error(socket, 500, &format!("Query execution failed: {}", e)).await?;
        }
    }

    Ok(())
}

async fn handle_file_route(
    socket: &mut TcpStream,
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
        send_error(socket, 400, "Path traversal not allowed").await?;
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

    // Read the file
    let file_content = match tokio::fs::read(&resolved_path).await {
        Ok(content) => content,
        Err(e) => {
            // Send error frame
            let err_msg = format!("Failed to read file: {}, error {}", resolved_path.display(), e);
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
            socket.write_all(&frame_e(&err_msg)).await?;
            socket.write_all(&frame_eof()).await?;
            return Ok(());
        }
    };

    // Apply lines limit if specified
    let final_content = if let Some(limit) = lines_limit {
        // Find the position of the N-th newline
        let mut line_count = 0;
        let mut end_pos = file_content.len();
        
        for (i, &byte) in file_content.iter().enumerate() {
            if byte == b'\n' {
                line_count += 1;
                if line_count >= limit {
                    end_pos = i + 1; // Include the newline
                    break;
                }
            }
        }
        
        &file_content[..end_pos]
    } else {
        &file_content[..]
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

    // Send frames: F, O, L, D, EOF
    socket.write_all(&frame_f(&decoded_path)).await?;
    socket.write_all(&frame_o(0)).await?;
    socket.write_all(&frame_l(1)).await?;
    socket.write_all(&frame_d(final_content)).await?;
    socket.write_all(&frame_eof()).await?;

    Ok(())
}

// Framing helper functions for gpfdist protocol 1
// Frame format: [type:1][length:4][line_or_offset:4][data...]

/// Create a frame header with type, length, and line/offset
fn frame_hdr(letter: u8, len: u32) -> BytesMut {
    let mut buf = BytesMut::with_capacity(9);
    buf.extend_from_slice(&[letter]);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf
}

/// Create an 'F' (filename) frame
fn frame_f(filename: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + filename.len());
    buf.extend_from_slice(&[b'F']);
    buf.extend_from_slice(&(filename.len() as u32).to_be_bytes());
    buf.extend_from_slice(filename.as_bytes());
    buf
}

/// Create an 'O' (offset) frame
fn frame_o(offset: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(9);
    buf.extend_from_slice(&[b'O']);
    buf.extend_from_slice(&8u32.to_be_bytes());
    buf.extend_from_slice(&(offset).to_be_bytes());
    buf
}

/// Create an 'L' (line number) frame
fn frame_l(line_no: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(9);
    buf.extend_from_slice(&[b'L']);
    buf.extend_from_slice(&8u32.to_be_bytes());
    buf.extend_from_slice(&(line_no).to_be_bytes());
    buf
}

/// Create a 'D' (data) frame
fn frame_d(data: &[u8]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + data.len());
    buf.extend_from_slice(&[b'D']);
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(data);
    buf
}

/// Create an 'E' (error) frame
fn frame_e(msg: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5 + msg.len());
    buf.extend_from_slice(&[b'E']);
    buf.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    buf.extend_from_slice(msg.as_bytes());
    buf
}

/// Create an EOF frame (D frame with length 0)
fn frame_eof() -> BytesMut {
    let mut buf = BytesMut::with_capacity(5);
    buf.extend_from_slice(&[b'D']);
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf
}

async fn send_ok(socket: &mut TcpStream, body: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.write_all(body).await?;
    Ok(())
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
        status, status_text, message.len(), message
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

/// Truncate data to the first N lines (newline-terminated)
fn truncate_to_lines(data: &[u8], lines: usize) -> Vec<u8> {
    if lines == 0 {
        return Vec::new();
    }
    
    let mut line_count = 0;
    for (i, &byte) in data.iter().enumerate() {
        if byte == b'\n' {
            line_count += 1;
            if line_count >= lines {
                return data[..=i].to_vec();
            }
        }
    }
    
    // If we haven't found enough newlines, return all data
    data.to_vec()
}