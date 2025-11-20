use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

const TEST_SERVER_ADDR: &str = "127.0.0.1:18080";

#[test]
#[ignore]
fn test_concurrent_segment_requests() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_concurrent_segment_requests_async().await;
    });
}

async fn test_concurrent_segment_requests_async() {
    // Create temp directory with multiple parquet files
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path();
    
    // Create 3 parquet files
    for i in 0..3 {
        let file_path = dir_path.join(format!("test_{}.parquet", i));
        create_test_parquet_file(&file_path, i * 10).unwrap();
    }

    // Start server in background
    let server_handle = start_test_server();
    sleep(Duration::from_millis(500)).await;

    // Make 5 concurrent requests to the same directory path
    // Each request simulates a different segment
    let mut handles = vec![];
    
    for seg_id in 0..5 {
        let path = dir_path.to_str().unwrap().to_string();
        let handle = tokio::spawn(async move {
            make_request_with_segment(&path, seg_id, 5).await
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    let mut all_succeeded = true;
    for handle in handles {
        match handle.await {
            Ok(success) => {
                if !success {
                    all_succeeded = false;
                    eprintln!("One of the requests failed");
                }
            }
            Err(e) => {
                all_succeeded = false;
                eprintln!("Task join error: {}", e);
            }
        }
    }

    assert!(all_succeeded, "All concurrent requests should succeed");

    // Cleanup
    drop(server_handle);
}

async fn make_request_with_segment(path: &str, seg_id: usize, seg_count: usize) -> bool {
    // Add a small random delay to increase chance of concurrent execution
    let delay_ms = (seg_id * 10) as u64;
    sleep(Duration::from_millis(delay_ms)).await;

    let url = format!("path={}", path);
    let request = format!(
        "GET /df/parquet?{} HTTP/1.1\r\n\
         Host: localhost:18080\r\n\
         X-GP-PROTO: 0\r\n\
         X-GP-SEGMENT-ID: {}\r\n\
         X-GP-SEGMENT-COUNT: {}\r\n\
         Connection: close\r\n\
         \r\n",
        url, seg_id, seg_count
    );

    match TcpStream::connect(TEST_SERVER_ADDR) {
        Ok(mut stream) => {
            if let Err(e) = stream.write_all(request.as_bytes()) {
                eprintln!("Failed to write request for segment {}: {}", seg_id, e);
                return false;
            }

            let mut response = String::new();
            if let Err(e) = stream.read_to_string(&mut response) {
                eprintln!("Failed to read response for segment {}: {}", seg_id, e);
                return false;
            }

            // Check for success
            if !response.starts_with("HTTP/1.1 200 OK") {
                eprintln!("Segment {} got non-200 response: {}", seg_id, response.lines().next().unwrap_or(""));
                return false;
            }

            // Check for the "table already exists" error
            if response.contains("already exists") {
                eprintln!("Segment {} got 'table already exists' error", seg_id);
                return false;
            }

            true
        }
        Err(e) => {
            eprintln!("Failed to connect for segment {}: {}", seg_id, e);
            false
        }
    }
}

fn create_test_parquet_file(path: &std::path::Path, id_offset: i32) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![
        id_offset + 1,
        id_offset + 2,
        id_offset + 3,
    ]);
    let name_array = StringArray::from(vec![
        format!("Name{}", id_offset + 1),
        format!("Name{}", id_offset + 2),
        format!("Name{}", id_offset + 3),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn start_test_server() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        std::env::set_var("GPFDIST_ADDR", TEST_SERVER_ADDR);
        let server = gpfdist_rs::Server::new(TEST_SERVER_ADDR.to_string());
        let _ = server.run().await;
    })
}
