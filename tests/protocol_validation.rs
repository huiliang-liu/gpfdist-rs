use std::io::{Read, Write};
use std::net::TcpStream;
use tempfile::TempDir;
use std::fs::File;

#[test]
#[ignore]
fn test_df_route_requires_gp_proto_1() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_df_route_requires_gp_proto_1_async().await;
    });
}

#[test]
#[ignore]
fn test_df_route_rejects_gp_proto_0() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_df_route_rejects_gp_proto_0_async().await;
    });
}

#[test]
#[ignore]
fn test_file_route_requires_gp_proto_1() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_route_requires_gp_proto_1_async().await;
    });
}

#[test]
#[ignore]
fn test_file_route_rejects_gp_proto_0() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_route_rejects_gp_proto_0_async().await;
    });
}

#[test]
#[ignore]
fn test_file_route_with_lines_limit() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_file_route_with_lines_limit_async().await;
    });
}

#[test]
#[ignore]
fn test_post_request_rejected() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_post_request_rejected_async().await;
    });
}

async fn test_df_route_requires_gp_proto_1_async() {
    // Create temp parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with X-GP-PROTO: 1
    let url = format!("files={}", file_path.to_str().unwrap());
    let request = format!(
        "GET /df/parquet?{} HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18080").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let response_str = String::from_utf8_lossy(&response);

    // Should succeed
    assert!(response_str.starts_with("HTTP/1.1 200 OK"), "Expected 200 OK, got: {}", response_str);

    drop(server_handle);
}

async fn test_df_route_rejects_gp_proto_0_async() {
    // Create temp parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with X-GP-PROTO: 0 (should be rejected)
    let url = format!("files={}", file_path.to_str().unwrap());
    let request = format!(
        "GET /df/parquet?{} HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18080").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Should return 400 Bad Request
    assert!(response.starts_with("HTTP/1.1 400 Bad Request"), "Expected 400, got: {}", response);
    assert!(response.contains("X-GP-PROTO must be 1 for GET"), "Expected error message about X-GP-PROTO");

    drop(server_handle);
}

async fn test_file_route_requires_gp_proto_1_async() {
    // Create temp CSV file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    create_test_csv_file(&file_path).unwrap();

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with X-GP-PROTO: 1
    let url = format!("/{}", file_path.to_str().unwrap());
    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18080").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    // Find header end
    let header_end = find_header_end(&response).unwrap();
    let response_header = String::from_utf8_lossy(&response[..header_end]);

    // Should succeed with 200 OK
    assert!(response_header.starts_with("HTTP/1.1 200 OK"), "Expected 200 OK, got: {}", response_header);

    // Verify frames are present
    let frames = &response[header_end..];
    assert!(!frames.is_empty(), "Should have frame data");
    assert_eq!(frames[0], b'F', "First frame should be F");

    drop(server_handle);
}

async fn test_file_route_rejects_gp_proto_0_async() {
    // Create temp CSV file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    create_test_csv_file(&file_path).unwrap();

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with X-GP-PROTO: 0 (should be rejected)
    let url = format!("/{}", file_path.to_str().unwrap());
    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18080").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Should return 400 Bad Request
    assert!(response.starts_with("HTTP/1.1 400 Bad Request"), "Expected 400, got: {}", response);
    assert!(response.contains("X-GP-PROTO must be 1 for GET"), "Expected error message about X-GP-PROTO");

    drop(server_handle);
}

async fn test_file_route_with_lines_limit_async() {
    // Create temp CSV file with multiple lines
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    create_test_csv_file(&file_path).unwrap();

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request with lines=2 parameter
    let url = format!("/{}?lines=2", file_path.to_str().unwrap());
    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18080").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    // Find header end
    let header_end = find_header_end(&response).unwrap();
    let frames = &response[header_end..];

    // Count D frames with data (excluding EOF)
    let mut data_frames = 0;
    let mut i = 0;
    
    while i < frames.len() {
        if i + 9 > frames.len() {
            break;
        }

        let frame_type = frames[i];
        let length = u32::from_be_bytes([frames[i + 1], frames[i + 2], frames[i + 3], frames[i + 4]]);

        match frame_type {
            b'F' | b'O' | b'L' => i += 9,
            b'D' => {
                if length > 0 {
                    data_frames += 1;
                }
                i += 9 + length as usize;
            }
            _ => break,
        }
    }

    // Should have exactly 2 data frames (limited by lines=2)
    assert_eq!(data_frames, 2, "Expected 2 data frames with lines=2, got {}", data_frames);

    drop(server_handle);
}

async fn test_post_request_rejected_async() {
    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make POST request
    let request = format!(
        "POST /df/parquet HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 0\r\n\
         Connection: close\r\n\
         \r\n"
    );

    let mut stream = TcpStream::connect("127.0.0.1:18080").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Should return 400 Bad Request
    assert!(response.starts_with("HTTP/1.1 400 Bad Request"), "Expected 400, got: {}", response);
    assert!(response.contains("only GET supported"), "Expected 'only GET supported' message");

    drop(server_handle);
}

// Helper functions

fn create_test_parquet_file(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array), Arc::new(name_array)])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn create_test_csv_file(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    
    let mut file = File::create(path)?;
    writeln!(file, "id,name")?;
    writeln!(file, "1,Alice")?;
    writeln!(file, "2,Bob")?;
    writeln!(file, "3,Charlie")?;
    
    Ok(())
}

fn start_test_server() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        std::env::set_var("GPFDIST_ADDR", "127.0.0.1:18080");
        let server = gpfdist_rs::Server::new("127.0.0.1:18080".to_string());
        let _ = server.run().await;
    })
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    for i in 0..data.len().saturating_sub(3) {
        if &data[i..i + 4] == b"\r\n\r\n" {
            return Some(i + 4);
        }
    }
    None
}
