use crate::df_engine::{DFEngine, DFRequest, TableType};
use crate::util::{parse_query_map, percent_decode};
use futures::StreamExt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

    // Route handling
    if method == "GET" {
        if path.starts_with("/df/") {
            handle_df_route(&mut socket, path, &headers, df_engine).await?;
        } else {
            // Fallback: serve file directly
            handle_file_route(&mut socket, path, &headers).await?;
        }
    } else {
        send_error(&mut socket, 405, "Method Not Allowed").await?;
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
    match df_engine.execute_to_gpfdist_stream(request).await {
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

            // Stream data
            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        socket.write_all(&chunk).await?;
                    }
                    Err(e) => {
                        eprintln!("Stream error: {}", e);
                        if gp_proto == 1 {
                            // Send error frame
                            let err_msg = format!("ERROR: {}", e);
                            let mut e_frame = vec![b'E'];
                            e_frame.extend_from_slice(&(err_msg.len() as u32).to_be_bytes());
                            e_frame.extend_from_slice(&0u32.to_be_bytes());
                            e_frame.extend_from_slice(err_msg.as_bytes());
                            socket.write_all(&e_frame).await?;
                            
                            // Send EOF
                            let eof = vec![b'D', 0, 0, 0, 0, 0, 0, 0, 0];
                            socket.write_all(&eof).await?;
                        }
                        break;
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

#[allow(dead_code)]
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

async fn handle_file_route(
    socket: &mut TcpStream,
    raw_path: &str,
    headers: &std::collections::HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse path and query parameters
    let path_without_query = raw_path.split('?').next().unwrap_or(raw_path);
    let query_map = parse_query_map(raw_path);
    
    // Percent-decode the path
    let decoded_path = percent_decode(path_without_query);
    
    // Security check: reject paths containing ".."
    if decoded_path.contains("..") {
        send_error(socket, 400, "Invalid path: directory traversal not allowed").await?;
        return Ok(());
    }
    
    // Resolve path (make absolute if relative)
    let file_path = if decoded_path.starts_with('/') {
        decoded_path.clone()
    } else {
        // Resolve relative to current working directory
        let cwd = std::env::current_dir()
            .map_err(|e| format!("Failed to get current directory: {}", e))?;
        cwd.join(&decoded_path)
            .to_string_lossy()
            .to_string()
    };
    
    // Parse gp_proto from header (default 0)
    let gp_proto = headers
        .get("x-gp-proto")
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);
    
    if gp_proto > 1 {
        send_error(socket, 400, "Invalid X-GP-PROTO (must be 0 or 1)").await?;
        return Ok(());
    }
    
    // Parse lines parameter
    let lines_limit = query_map.get("lines").and_then(|s| s.parse::<usize>().ok());
    
    // Try to read the file
    let file_data = match tokio::fs::read(&file_path).await {
        Ok(data) => data,
        Err(e) => {
            if gp_proto == 1 {
                // For proto 1, send error frame
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/octet-stream\r\n\
                     X-GP-PROTO: 1\r\n\
                     Connection: close\r\n\
                     \r\n"
                );
                socket.write_all(response.as_bytes()).await?;
                
                let err_msg = format!("Failed to read file {}: {}", file_path, e);
                let e_frame = frame_e(&err_msg);
                socket.write_all(&e_frame).await?;
                
                let eof_frame = frame_eof();
                socket.write_all(&eof_frame).await?;
            } else {
                // For proto 0, send HTTP error
                let status = if e.kind() == std::io::ErrorKind::NotFound {
                    404
                } else {
                    500
                };
                let msg = format!("Failed to read file: {}", e);
                send_error(socket, status, &msg).await?;
            }
            return Ok(());
        }
    };
    
    // Apply lines limit if specified
    let output_data = if let Some(limit) = lines_limit {
        truncate_to_lines(&file_data, limit)
    } else {
        file_data
    };
    
    // Send response based on gp_proto
    if gp_proto == 0 {
        // Protocol 0: HTTP OK + raw bytes
        let response = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: application/octet-stream\r\n\
             X-GP-PROTO: 0\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            output_data.len()
        );
        socket.write_all(response.as_bytes()).await?;
        socket.write_all(&output_data).await?;
    } else {
        // Protocol 1: HTTP OK + framed output
        let response = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: application/octet-stream\r\n\
             X-GP-PROTO: 1\r\n\
             Connection: close\r\n\
             \r\n"
        );
        socket.write_all(response.as_bytes()).await?;
        
        // Send F frame (filename)
        let f_frame = frame_f(&decoded_path);
        socket.write_all(&f_frame).await?;
        
        // Send O frame (offset = 0)
        let o_frame = frame_o(0);
        socket.write_all(&o_frame).await?;
        
        // Send L frame (line start = 1)
        let l_frame = frame_l(1);
        socket.write_all(&l_frame).await?;
        
        // Send D frame (data)
        let d_frame = frame_d(&output_data);
        socket.write_all(&d_frame).await?;
        
        // Send EOF frame
        let eof_frame = frame_eof();
        socket.write_all(&eof_frame).await?;
    }
    
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

// ========== gpfdist protocol framing helpers ==========

/// Create a frame header: [type:1][length:4][line_or_offset:4]
#[allow(dead_code)]
fn frame_hdr(letter: u8, len: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(letter);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes()); // Reserved field
    buf
}

/// Create F frame (filename)
fn frame_f(filename: &str) -> Vec<u8> {
    let filename_bytes = filename.as_bytes();
    let mut frame = Vec::with_capacity(9 + filename_bytes.len());
    frame.push(b'F');
    frame.extend_from_slice(&(filename_bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(filename_bytes);
    frame
}

/// Create O frame (offset)
fn frame_o(offset: u64) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(b'O');
    frame.extend_from_slice(&(offset as u32).to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame
}

/// Create L frame (line number)
fn frame_l(line_no: u64) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(b'L');
    frame.extend_from_slice(&(line_no as u32).to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame
}

/// Create D frame (data)
fn frame_d(data: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9 + data.len());
    frame.push(b'D');
    frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
    frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
    frame.extend_from_slice(data);
    frame
}

/// Create E frame (error message)
fn frame_e(msg: &str) -> Vec<u8> {
    let msg_bytes = msg.as_bytes();
    let mut frame = Vec::with_capacity(9 + msg_bytes.len());
    frame.push(b'E');
    frame.extend_from_slice(&(msg_bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(msg_bytes);
    frame
}

/// Create EOF frame (D frame with length 0)
fn frame_eof() -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(b'D');
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame
}
