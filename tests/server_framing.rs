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
fn test_server_framing_single_fol() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_server_framing_single_fol_async().await;
    });
}

async fn test_server_framing_single_fol_async() {
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

    // Parse frames and verify structure
    assert!(!frames.is_empty(), "Should have frame data");

    // Count frame occurrences
    let mut f_count = 0;
    let mut o_count = 0;
    let mut l_count = 0;
    let mut d_count = 0;
    let mut eof_count = 0;

    let mut i = 0;
    while i < frames.len() {
        if i + 5 > frames.len() {
            break;
        }

        let frame_type = frames[i];
        let length =
            u32::from_be_bytes([frames[i + 1], frames[i + 2], frames[i + 3], frames[i + 4]]);

        match frame_type {
            b'F' => {
                f_count += 1;
                // F frame format: type(1) + length(4) + data
                i += 5 + length as usize;
            }
            b'O' => {
                o_count += 1;
                // O frame format: type(1) + length(4) + offset(8)
                i += 5 + length as usize;
            }
            b'L' => {
                l_count += 1;
                // L frame format: type(1) + length(4) + line_no(8)
                i += 5 + length as usize;
            }
            b'D' => {
                if length == 0 {
                    eof_count += 1;
                } else {
                    d_count += 1;
                }
                // D frame format: type(1) + length(4) + data
                i += 5 + length as usize;
            }
            _ => break,
        }
    }

    println!(
        "Frame counts: F={}, O={}, L={}, D={}, EOF={}",
        f_count, o_count, l_count, d_count, eof_count
    );

    // Verify: F/O/L frames should appear exactly once
    assert_eq!(f_count, 1, "Should have exactly one F frame");
    assert_eq!(o_count, 1, "Should have exactly one O frame");
    assert_eq!(l_count, 1, "Should have exactly one L frame");

    // Verify: At least one D frame with data
    assert!(d_count > 0, "Should have at least one D frame with data");

    // Verify: Exactly one EOF frame
    assert_eq!(eof_count, 1, "Should have exactly one EOF frame");

    // Verify frame order: F, O, L should come before any D frames
    i = 0;
    let mut seen_d = false;
    let mut f_before_d = false;
    let mut o_before_d = false;
    let mut l_before_d = false;

    while i < frames.len() {
        if i + 5 > frames.len() {
            break;
        }

        let frame_type = frames[i];
        let length =
            u32::from_be_bytes([frames[i + 1], frames[i + 2], frames[i + 3], frames[i + 4]]);

        match frame_type {
            b'F' => {
                if !seen_d {
                    f_before_d = true;
                }
                i += 5 + length as usize;
            }
            b'O' => {
                if !seen_d {
                    o_before_d = true;
                }
                i += 5 + length as usize;
            }
            b'L' => {
                if !seen_d {
                    l_before_d = true;
                }
                i += 5 + length as usize;
            }
            b'D' => {
                if length > 0 {
                    seen_d = true;
                }
                i += 5 + length as usize;
            }
            _ => break,
        }
    }

    assert!(f_before_d, "F frame should come before D frames");
    assert!(o_before_d, "O frame should come before D frames");
    assert!(l_before_d, "L frame should come before D frames");

    // Cleanup
    drop(server_handle);
}

fn create_test_parquet_file(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

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
