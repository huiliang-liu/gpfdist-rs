use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
#[ignore]
fn test_parquet_proto_0() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_parquet_proto_0_async().await;
    });
}

#[test]
#[ignore]
fn test_parquet_proto_1() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_parquet_proto_1_async().await;
    });
}

#[test]
#[ignore]
fn test_parquet_with_projection() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_parquet_with_projection_async().await;
    });
}

#[test]
#[ignore]
fn test_parquet_with_filter() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_parquet_with_filter_async().await;
    });
}

#[test]
#[ignore]
fn test_parquet_with_limit() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_parquet_with_limit_async().await;
    });
}

async fn test_parquet_proto_0_async() {
    // Create temp directory and parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
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

    // Validate response
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("X-GP-PROTO: 0"));

    // Extract body (CSV data)
    let body_start = response.find("\r\n\r\n").unwrap() + 4;
    let body = &response[body_start..];

    // Should contain CSV data with 3 rows
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 3, "Expected 3 CSV rows");

    // Check content (id,name columns)
    assert!(lines[0].contains("1") && lines[0].contains("Alice"));
    assert!(lines[1].contains("2") && lines[1].contains("Bob"));
    assert!(lines[2].contains("3") && lines[2].contains("Charlie"));

    // Cleanup
    drop(server_handle);
}

async fn test_parquet_proto_1_async() {
    // Create temp directory and parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server in background
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
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

    // Find header end
    let header_end = find_header_end(&response).unwrap();
    let frames = &response[header_end..];

    // Parse frames
    assert!(!frames.is_empty(), "Should have frame data");

    // Check for F frame (File header)
    assert_eq!(frames[0], b'F', "First frame should be F");

    // Check for EOF frame (D with length 0)
    let has_eof = frames
        .windows(9)
        .any(|w| w[0] == b'D' && w[1..5] == [0, 0, 0, 0]);
    assert!(has_eof, "Should have EOF frame (D with length 0)");

    // Count O, L, D frames (excluding EOF)
    let mut has_o_frame = false;
    let mut has_l_frame = false;
    let mut has_d_frame = false;

    let mut i = 0;
    while i < frames.len() {
        if i + 9 > frames.len() {
            break;
        }

        let frame_type = frames[i];
        let length = u32::from_be_bytes([frames[i + 1], frames[i + 2], frames[i + 3], frames[i + 4]]);

        match frame_type {
            b'F' => i += 9,
            b'O' => {
                has_o_frame = true;
                i += 9;
            }
            b'L' => {
                has_l_frame = true;
                i += 9;
            }
            b'D' => {
                if length > 0 {
                    has_d_frame = true;
                }
                i += 9 + length as usize;
            }
            _ => break,
        }
    }

    assert!(has_o_frame, "Should have O frame");
    assert!(has_l_frame, "Should have L frame");
    assert!(has_d_frame, "Should have D frame with data");

    // Cleanup
    drop(server_handle);
}

async fn test_parquet_with_projection_async() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let url = format!("files={}&columns=name", file_path.to_str().unwrap());
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

    let body_start = response.find("\r\n\r\n").unwrap() + 4;
    let body = &response[body_start..];

    // Should only contain name column
    assert!(body.contains("Alice"));
    assert!(body.contains("Bob"));
    // Should NOT contain id values in separate columns
    let lines: Vec<&str> = body.lines().collect();
    assert!(lines.len() >= 3);

    drop(server_handle);
}

async fn test_parquet_with_filter_async() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Filter: id > 1
    let url = format!(
        "files={}&filter=id%20%3E%201",
        file_path.to_str().unwrap()
    );
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

    let body_start = response.find("\r\n\r\n").unwrap() + 4;
    let body = &response[body_start..];

    // Should contain Bob and Charlie but not Alice
    assert!(body.contains("Bob"));
    assert!(body.contains("Charlie"));
    assert!(!body.contains("Alice"));

    drop(server_handle);
}

async fn test_parquet_with_limit_async() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let url = format!("files={}&limit=1", file_path.to_str().unwrap());
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

    let body_start = response.find("\r\n\r\n").unwrap() + 4;
    let body = &response[body_start..];

    // Should only have 1 row
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 1);

    drop(server_handle);
}

fn create_test_parquet_file(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
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
