// Tests for per-packet F/O/L meta framing behavior

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Debug, PartialEq)]
struct Frame {
    frame_type: u8,
    data: Vec<u8>,
}

fn parse_frames(data: &[u8]) -> Vec<Frame> {
    let mut frames = Vec::new();
    let mut i = 0;

    while i < data.len() {
        if i + 5 > data.len() {
            break;
        }

        let frame_type = data[i];
        let length = u32::from_be_bytes([data[i + 1], data[i + 2], data[i + 3], data[i + 4]]) as usize;

        if i + 5 + length > data.len() {
            break;
        }

        let frame_data = data[i + 5..i + 5 + length].to_vec();
        frames.push(Frame {
            frame_type,
            data: frame_data,
        });

        i += 5 + length;
    }

    frames
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    for i in 0..data.len().saturating_sub(3) {
        if &data[i..i + 4] == b"\r\n\r\n" {
            return Some(i + 4);
        }
    }
    None
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

fn start_test_server(port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let addr = format!("127.0.0.1:{}", port);
        let server = gpfdist_rs::Server::new(addr.clone());
        let _ = server.run().await;
    })
}

#[tokio::test]
#[ignore] // Integration test
async fn test_per_packet_meta_frames() {
    // Create temp directory and parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server in background
    let _server_handle = start_test_server(18081);
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
    let url = format!("files={}", file_path.to_str().unwrap());
    let request = format!(
        "GET /df/parquet?{} HTTP/1.1\r\n\
         Host: localhost:18081\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18081").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    // Find header end
    let header_end = find_header_end(&response).unwrap();
    let frame_data = &response[header_end..];

    // Parse frames
    let frames = parse_frames(frame_data);

    // Verify frame structure: each D frame should be preceded by F/O/L
    // Pattern should be: F O L D [F O L D]* F O L D(EOF)
    
    let mut i = 0;
    let mut data_packets = 0;
    let mut eof_found = false;

    while i < frames.len() {
        // Every packet should start with F O L
        if i + 2 >= frames.len() {
            break;
        }

        assert_eq!(frames[i].frame_type, b'F', "Expected F frame at position {}", i);
        assert_eq!(frames[i + 1].frame_type, b'O', "Expected O frame at position {}", i + 1);
        assert_eq!(frames[i + 2].frame_type, b'L', "Expected L frame at position {}", i + 2);

        // After F O L, we should have D frame
        if i + 3 < frames.len() {
            assert_eq!(frames[i + 3].frame_type, b'D', "Expected D frame at position {}", i + 3);
            
            if frames[i + 3].data.is_empty() {
                eof_found = true;
                break; // EOF is zero-length D
            } else {
                data_packets += 1;
            }
            
            i += 4; // Move past F O L D
        } else {
            break;
        }
    }

    assert!(data_packets > 0, "Should have at least one data packet");
    assert!(eof_found, "Should have EOF packet");
}

#[tokio::test]
#[ignore] // Integration test
async fn test_offset_counting() {
    // Create temp directory and parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server
    let _server_handle = start_test_server(18082);
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
    let url = format!("files={}", file_path.to_str().unwrap());
    let request = format!(
        "GET /df/parquet?{} HTTP/1.1\r\n\
         Host: localhost:18082\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18082").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    let header_end = find_header_end(&response).unwrap();
    let frame_data = &response[header_end..];
    let frames = parse_frames(frame_data);

    // Track offsets across packets
    let mut expected_offset = 0u64;
    let mut i = 0;

    while i < frames.len() {
        if i + 3 >= frames.len() {
            break;
        }

        if frames[i].frame_type == b'F' && frames[i + 1].frame_type == b'O' {
            // Parse offset from O frame
            let offset_bytes: [u8; 8] = frames[i + 1].data.as_slice().try_into().unwrap();
            let offset = u64::from_be_bytes(offset_bytes);
            
            assert_eq!(offset, expected_offset, "Offset should be cumulative CSV bytes only");

            // If D frame has data, update expected offset
            if i + 3 < frames.len() && frames[i + 3].frame_type == b'D' && !frames[i + 3].data.is_empty() {
                expected_offset += frames[i + 3].data.len() as u64;
            }
        }

        i += 1;
    }
}

#[tokio::test]
#[ignore] // Integration test
async fn test_line_number_counting() {
    // Create temp directory with CSV data
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");
    
    // Write CSV with known line count
    std::fs::write(&file_path, "line1\nline2\nline3").unwrap();

    // Start server
    let _server_handle = start_test_server(18083);
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request for file route
    let path = file_path.to_str().unwrap();
    let request = format!(
        "GET {} HTTP/1.1\r\n\
         Host: localhost:18083\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        path
    );

    let mut stream = TcpStream::connect("127.0.0.1:18083").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    let header_end = find_header_end(&response).unwrap();
    let frame_data = &response[header_end..];
    let frames = parse_frames(frame_data);

    // Find the EOF packet (last F O L D with zero-length D)
    let mut eof_line_no = 0u64;
    let mut i = frames.len();
    
    while i >= 4 {
        i -= 1;
        if frames[i].frame_type == b'D' && frames[i].data.is_empty() {
            // Found EOF, get line number from L frame (should be 2 positions back)
            if i >= 2 && frames[i - 1].frame_type == b'L' {
                let line_bytes: [u8; 8] = frames[i - 1].data.as_slice().try_into().unwrap();
                eof_line_no = u64::from_be_bytes(line_bytes);
                break;
            }
        }
    }

    // The file has 3 lines but only 2 newlines, so final line should be counted at EOF
    // Final line_no should be 4 (started at 1, saw 2 newlines, counted final partial line)
    assert_eq!(eof_line_no, 4, "Line number should count partial line at EOF");
}

#[tokio::test]
#[ignore] // Integration test  
async fn test_eof_packet_structure() {
    // Create temp directory and parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    create_test_parquet_file(&file_path).unwrap();

    // Start server
    let _server_handle = start_test_server(18084);
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Make request
    let url = format!("files={}", file_path.to_str().unwrap());
    let request = format!(
        "GET /df/parquet?{} HTTP/1.1\r\n\
         Host: localhost:18084\r\n\
         X-GP-PROTO: 1\r\n\
         Connection: close\r\n\
         \r\n",
        url
    );

    let mut stream = TcpStream::connect("127.0.0.1:18084").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    let header_end = find_header_end(&response).unwrap();
    let frame_data = &response[header_end..];
    let frames = parse_frames(frame_data);

    // Find the EOF packet (should be last 4 frames: F O L D(0))
    assert!(frames.len() >= 4, "Should have at least 4 frames for EOF");
    
    let eof_start = frames.len() - 4;
    assert_eq!(frames[eof_start].frame_type, b'F', "EOF should start with F frame");
    assert_eq!(frames[eof_start + 1].frame_type, b'O', "EOF should have O frame");
    assert_eq!(frames[eof_start + 2].frame_type, b'L', "EOF should have L frame");
    assert_eq!(frames[eof_start + 3].frame_type, b'D', "EOF should end with D frame");
    assert!(frames[eof_start + 3].data.is_empty(), "EOF D frame should be zero-length");
}
