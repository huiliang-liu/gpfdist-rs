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
        // Validate X-GP-PROTO header for GET requests
        let gp_proto = headers
            .get("x-gp-proto")
            .and_then(|s| s.parse::<u8>().ok())
            .unwrap_or(0);
        
        if gp_proto != 1 {
            send_error(&mut socket, 400, "X-GP-PROTO must be 1 for GET").await?;
            return Ok(());
        }

        if path.starts_with("/df/") {
            handle_df_route(&mut socket, path, &headers, df_engine).await?;
        } else {
            // Handle file serving with optional lines limit
            handle_file_route(&mut socket, path, &headers).await?;
        }
    } else if method == "POST" {
        // POST not supported yet, but validate X-GP-PROTO would need to be 0
        send_error(&mut socket, 400, "only GET supported").await?;
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

async fn handle_file_route(
    socket: &mut TcpStream,
    path: &str,
    _headers: &std::collections::HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs::File;
    use tokio::io::{AsyncBufReadExt, BufReader};

    // Parse file path and query parameters
    let (file_path, query_str) = if let Some(pos) = path.find('?') {
        (&path[1..pos], &path[pos + 1..])
    } else {
        (&path[1..], "")
    };

    // Parse query parameters for lines limit
    let query_map = parse_query_map(&format!("?{}", query_str));
    let lines_limit = query_map
        .get("lines")
        .and_then(|s| s.parse::<usize>().ok());

    // Decode the file path
    let decoded_path = percent_decode(file_path);
    
    // Try to open the file
    let file = match File::open(&decoded_path).await {
        Ok(f) => f,
        Err(e) => {
            send_error(socket, 404, &format!("File not found: {}", e)).await?;
            return Ok(());
        }
    };

    // Always use gp_proto=1 for file serving (framed output)
    let gp_proto = 1;

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

    // Send F frame (File header)
    let f_frame = vec![b'F', 0, 0, 0, 0, 0, 0, 0, 0];
    socket.write_all(&f_frame).await?;

    // Send O frame (Offset)
    let o_frame = vec![b'O', 0, 0, 0, 0, 0, 0, 0, 0];
    socket.write_all(&o_frame).await?;

    // Send L frame (Line number)
    let l_frame = vec![b'L', 0, 0, 0, 0, 0, 0, 0, 0];
    socket.write_all(&l_frame).await?;

    // Read and send file content line by line
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut line_count = 0;

    while let Some(line) = lines.next_line().await? {
        if let Some(limit) = lines_limit {
            if line_count >= limit {
                break;
            }
        }

        // Send D frame with CSV data (line + newline)
        let mut data = line.into_bytes();
        data.push(b'\n');
        
        let mut d_frame = vec![b'D'];
        d_frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
        d_frame.extend_from_slice(&0u32.to_be_bytes());
        d_frame.extend_from_slice(&data);
        
        socket.write_all(&d_frame).await?;
        line_count += 1;
    }

    // Send EOF frame (D with length 0)
    let eof = vec![b'D', 0, 0, 0, 0, 0, 0, 0, 0];
    socket.write_all(&eof).await?;

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
