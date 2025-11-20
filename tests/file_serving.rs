use std::io::{Read, Write};
use std::net::TcpStream;
use tempfile::TempDir;

/// Helper function to start the server
fn start_test_server() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        let server = gpfdist_rs::Server::new("127.0.0.1:18081".to_string());
        let _ = server.run().await;
    })
}

#[test]
#[ignore]
fn test_file_serving_proto_0_whole_file() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_serving_proto_0_whole_file_async().await;
    });
}

#[test]
#[ignore]
fn test_file_serving_proto_0_with_lines() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_serving_proto_0_with_lines_async().await;
    });
}

#[test]
#[ignore]
fn test_file_serving_proto_1_whole_file() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_serving_proto_1_whole_file_async().await;
    });
}

#[test]
#[ignore]
fn test_file_serving_proto_1_with_lines() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_serving_proto_1_with_lines_async().await;
    });
}

#[test]
#[ignore]
fn test_file_not_found_proto_0() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_not_found_proto_0_async().await;
    });
}

#[test]
#[ignore]
fn test_file_not_found_proto_1() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_not_found_proto_1_async().await;
    });
}

#[test]
#[ignore]
fn test_path_traversal_rejected() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_path_traversal_rejected_async().await;
    });
}

async fn test_file_serving_proto_0_whole_file_async() {
    // Create temp file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    let test_content = "line1\nline2\nline3\n";
    std::fs::write(&file_path, test_content).unwrap();

    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n",
        file_path.to_str().unwrap()
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Validate response
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("X-GP-PROTO: 0"));

    // Extract body
    let body_start = response.find("\r\n\r\n").unwrap() + 4;
    let body = &response[body_start..];

    // Should contain all content
    assert_eq!(body, test_content);

    // Cleanup
    server_handle.abort();
}

async fn test_file_serving_proto_0_with_lines_async() {
    // Create temp file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    let test_content = "line1\nline2\nline3\nline4\nline5\n";
    std::fs::write(&file_path, test_content).unwrap();

    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with lines=2
    let request = format!(
        "GET {}?lines=2 HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n",
        file_path.to_str().unwrap()
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Validate response
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("X-GP-PROTO: 0"));

    // Extract body
    let body_start = response.find("\r\n\r\n").unwrap() + 4;
    let body = &response[body_start..];

    // Should contain only first 2 lines
    assert_eq!(body, "line1\nline2\n");

    // Cleanup
    server_handle.abort();
}

async fn test_file_serving_proto_1_whole_file_async() {
    // Create temp file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    let test_content = b"line1\nline2\n";
    std::fs::write(&file_path, test_content).unwrap();

    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        file_path.to_str().unwrap()
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    // Parse HTTP headers
    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap() + 4;
    let headers = String::from_utf8_lossy(&response[..header_end]);
    
    assert!(headers.contains("HTTP/1.1 200 OK"));
    assert!(headers.contains("X-GP-PROTO: 1"));

    // Check for frames in body
    let body = &response[header_end..];
    
    // Should start with F frame
    assert_eq!(body[0], b'F');
    
    // Should contain O, L, D frames
    let has_o_frame = body.windows(1).any(|w| w[0] == b'O');
    let has_l_frame = body.windows(1).any(|w| w[0] == b'L');
    let has_d_frame = body.windows(1).any(|w| w[0] == b'D');
    
    assert!(has_o_frame, "Should contain O frame");
    assert!(has_l_frame, "Should contain L frame");
    assert!(has_d_frame, "Should contain D frame");

    // Cleanup
    server_handle.abort();
}

async fn test_file_serving_proto_1_with_lines_async() {
    // Create temp file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    let test_content = b"line1\nline2\nline3\n";
    std::fs::write(&file_path, test_content).unwrap();

    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with lines=1
    let request = format!(
        "GET {}?lines=1 HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        file_path.to_str().unwrap()
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    // Parse HTTP headers
    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap() + 4;
    let headers = String::from_utf8_lossy(&response[..header_end]);
    
    assert!(headers.contains("HTTP/1.1 200 OK"));
    assert!(headers.contains("X-GP-PROTO: 1"));

    // The data frame should only contain "line1\n" (not line2 or line3)
    let body = &response[header_end..];
    let data_str = String::from_utf8_lossy(body);
    assert!(!data_str.contains("line2"), "Should not contain line2");
    assert!(!data_str.contains("line3"), "Should not contain line3");

    // Cleanup
    server_handle.abort();
}

async fn test_file_not_found_proto_0_async() {
    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request for non-existent file
    let request = format!(
        "GET /tmp/nonexistent-file-12345.txt HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n"
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Should get 404 error
    assert!(response.starts_with("HTTP/1.1 404"));

    // Cleanup
    server_handle.abort();
}

async fn test_file_not_found_proto_1_async() {
    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request for non-existent file
    let request = format!(
        "GET /tmp/nonexistent-file-12345.txt HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n"
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    // Should get 200 OK (proto 1 sends errors as frames)
    let headers = String::from_utf8_lossy(&response[..100]);
    assert!(headers.contains("HTTP/1.1 200 OK"));

    // Should contain E frame in body
    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap() + 4;
    let body = &response[header_end..];
    
    assert_eq!(body[0], b'E', "Should start with E frame for error");

    // Cleanup
    server_handle.abort();
}

async fn test_path_traversal_rejected_async() {
    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with path traversal
    let request = format!(
        "GET /tmp/../etc/passwd HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n"
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Should get 400 bad request
    assert!(response.starts_with("HTTP/1.1 400"));
    assert!(response.contains("directory traversal"));

    // Cleanup
    server_handle.abort();
}
