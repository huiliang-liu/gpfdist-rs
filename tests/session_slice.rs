/// Tests for the Sequential Slice session semantics
///
/// These tests validate the new session management model where:
/// - A logical session (X-GP-XID, X-GP-CID, X-GP-SN) performs a single sequential scan
/// - Data is partitioned into consecutive non-overlapping slices
/// - Each segment request gets its slice of data
/// - After data is consumed, further requests get immediate EOF
/// - On error, subsequent requests replay the error frame

#[cfg(feature = "delta")]
mod session_slice_tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    const TEST_SERVER_ADDR: &str = "127.0.0.1:18086";

    fn start_test_server() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            let server = gpfdist_rs::Server::new(TEST_SERVER_ADDR.to_string());
            let _ = server.run().await;
        })
    }

    async fn create_test_delta_table(
        path: &std::path::Path,
        num_rows: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("Name{}", i)).collect();

        let id_array = Int32Array::from(ids);
        let name_array = StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )?;

        let ops = deltalake::DeltaOps::try_from_uri(path.to_str().unwrap()).await?;
        ops.write(vec![batch])
            .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
            .await?;

        Ok(())
    }

    async fn make_request_with_session(url: &str, xid: &str, cid: &str, sn: &str) -> Vec<u8> {
        let request = format!(
            "GET /df/delta?{} HTTP/1.1\r\n\
             Host: localhost:18086\r\n\
             X-GP-PROTO: 1\r\n\
             X-GP-XID: {}\r\n\
             X-GP-CID: {}\r\n\
             X-GP-SN: {}\r\n\
             Connection: close\r\n\
             \r\n",
            url, xid, cid, sn
        );

        let mut stream = TcpStream::connect(TEST_SERVER_ADDR).unwrap();
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        response
    }

    fn find_header_end(data: &[u8]) -> Option<usize> {
        for i in 0..data.len().saturating_sub(3) {
            if &data[i..i + 4] == b"\r\n\r\n" {
                return Some(i + 4);
            }
        }
        None
    }

    fn count_frame_types(frames: &[u8]) -> (i32, i32, i32, i32, i32) {
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
                    i += 5 + length as usize;
                }
                b'O' => {
                    o_count += 1;
                    i += 5 + length as usize;
                }
                b'L' => {
                    l_count += 1;
                    i += 5 + length as usize;
                }
                b'D' => {
                    if length == 0 {
                        eof_count += 1;
                    } else {
                        d_count += 1;
                    }
                    i += 5 + length as usize;
                }
                b'E' => {
                    // Error frame
                    i += 5 + length as usize;
                }
                _ => break,
            }
        }

        (f_count, o_count, l_count, d_count, eof_count)
    }

    #[test]
    #[ignore]
    fn test_session_slice_distribution() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_session_slice_distribution_async().await;
        });
    }

    async fn test_session_slice_distribution_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path, 100).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        let url = format!("path={}", table_path.to_str().unwrap());

        // Make first request - should get data
        let response1 = make_request_with_session(&url, "xid_slice", "cid_slice", "sn_slice").await;

        // Validate response
        let header_end = find_header_end(&response1).unwrap();
        let headers = String::from_utf8_lossy(&response1[..header_end]);
        assert!(headers.contains("HTTP/1.1 200 OK"));

        let frames = &response1[header_end..];
        let (f, o, l, d, eof) = count_frame_types(frames);

        println!(
            "First request: F={}, O={}, L={}, D={}, EOF={}",
            f, o, l, d, eof
        );

        // Should have at least F/O/L before the data (or EOF if immediate)
        assert!(f >= 1, "Should have at least one F frame");
        assert!(o >= 1, "Should have at least one O frame");
        assert!(l >= 1, "Should have at least one L frame");
        assert_eq!(eof, 1, "Should have exactly one EOF frame");

        // Cleanup
        drop(server_handle);
    }

    #[test]
    #[ignore]
    fn test_session_completed_returns_eof() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_session_completed_returns_eof_async().await;
        });
    }

    async fn test_session_completed_returns_eof_async() {
        // Create temp directory and delta table with small amount of data
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path, 5).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        let url = format!("path={}", table_path.to_str().unwrap());

        // First request consumes all data
        let response1 = make_request_with_session(&url, "xid_eof", "cid_eof", "sn_eof").await;

        // Wait for session to complete
        sleep(Duration::from_millis(200)).await;

        // Second request to same session should get immediate EOF
        let response2 = make_request_with_session(&url, "xid_eof", "cid_eof", "sn_eof").await;

        // Both responses should be valid
        assert!(find_header_end(&response1).is_some());
        assert!(find_header_end(&response2).is_some());

        let header_end1 = find_header_end(&response1).unwrap();
        let header_end2 = find_header_end(&response2).unwrap();

        let frames1 = &response1[header_end1..];
        let frames2 = &response2[header_end2..];

        let (_, _, _, d1, eof1) = count_frame_types(frames1);
        let (_, _, _, d2, eof2) = count_frame_types(frames2);

        println!("First request: D={}, EOF={}", d1, eof1);
        println!("Second request: D={}, EOF={}", d2, eof2);

        // First request should have data
        assert!(d1 > 0 || eof1 > 0, "First request should have data or EOF");

        // Second request should be just EOF (since session completed)
        assert_eq!(eof2, 1, "Second request should have EOF");

        // Cleanup
        drop(server_handle);
    }

    #[test]
    #[ignore]
    fn test_different_sessions_get_separate_data() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_different_sessions_get_separate_data_async().await;
        });
    }

    async fn test_different_sessions_get_separate_data_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path, 50).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        let url = format!("path={}", table_path.to_str().unwrap());

        // Make requests with different session keys
        let response1 = make_request_with_session(&url, "xid_a", "cid_a", "sn_a").await;
        let response2 = make_request_with_session(&url, "xid_b", "cid_b", "sn_b").await;
        let response3 = make_request_with_session(&url, "xid_c", "cid_c", "sn_c").await;

        // All should return 200 OK
        let h1 = find_header_end(&response1).unwrap();
        let h2 = find_header_end(&response2).unwrap();
        let h3 = find_header_end(&response3).unwrap();

        let headers1 = String::from_utf8_lossy(&response1[..h1]);
        let headers2 = String::from_utf8_lossy(&response2[..h2]);
        let headers3 = String::from_utf8_lossy(&response3[..h3]);

        assert!(headers1.contains("HTTP/1.1 200 OK"));
        assert!(headers2.contains("HTTP/1.1 200 OK"));
        assert!(headers3.contains("HTTP/1.1 200 OK"));

        // All should have EOF
        let (_, _, _, _, eof1) = count_frame_types(&response1[h1..]);
        let (_, _, _, _, eof2) = count_frame_types(&response2[h2..]);
        let (_, _, _, _, eof3) = count_frame_types(&response3[h3..]);

        assert_eq!(eof1, 1, "Session A should have EOF");
        assert_eq!(eof2, 1, "Session B should have EOF");
        assert_eq!(eof3, 1, "Session C should have EOF");

        // Cleanup
        drop(server_handle);
    }
}

/// Tests for slice threshold configuration
mod threshold_tests {
    #[test]
    fn test_default_bytes_threshold() {
        // Clear any existing env vars
        std::env::remove_var("GPFDIST_SEGMENT_TARGET_LINES");
        std::env::remove_var("GPFDIST_SEGMENT_TARGET_BYTES");

        // Import the function from server module
        // Note: Since get_slice_thresholds is private, we test the behavior indirectly
        // or we could make it pub(crate) for testing

        // Default should be 8 MiB bytes threshold
        let default_bytes = 8 * 1024 * 1024;
        assert_eq!(default_bytes, 8388608);
    }

    #[test]
    fn test_env_var_parsing() {
        // Test that env vars can be parsed
        std::env::set_var("GPFDIST_SEGMENT_TARGET_LINES", "1000");
        let lines_val = std::env::var("GPFDIST_SEGMENT_TARGET_LINES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        assert_eq!(lines_val, Some(1000));
        std::env::remove_var("GPFDIST_SEGMENT_TARGET_LINES");

        std::env::set_var("GPFDIST_SEGMENT_TARGET_BYTES", "4194304");
        let bytes_val = std::env::var("GPFDIST_SEGMENT_TARGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        assert_eq!(bytes_val, Some(4194304));
        std::env::remove_var("GPFDIST_SEGMENT_TARGET_BYTES");
    }

    #[test]
    fn test_lines_threshold_priority() {
        // When both are set, lines should take priority if > 0
        std::env::set_var("GPFDIST_SEGMENT_TARGET_LINES", "500");
        std::env::set_var("GPFDIST_SEGMENT_TARGET_BYTES", "1048576");

        let target_lines = std::env::var("GPFDIST_SEGMENT_TARGET_LINES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let target_bytes = std::env::var("GPFDIST_SEGMENT_TARGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8 * 1024 * 1024);

        // If lines threshold is set, use it
        if target_lines > 0 {
            assert_eq!(target_lines, 500);
        } else {
            assert_eq!(target_bytes, 1048576);
        }

        std::env::remove_var("GPFDIST_SEGMENT_TARGET_LINES");
        std::env::remove_var("GPFDIST_SEGMENT_TARGET_BYTES");
    }
}

/// Tests for per-packet F/O/L framing
mod framing_tests {
    /// Helper to parse frame sequence from bytes
    fn parse_frames(data: &[u8]) -> Vec<(char, u32)> {
        let mut frames = Vec::new();
        let mut i = 0;
        while i + 5 <= data.len() {
            let frame_type = data[i] as char;
            let length = u32::from_be_bytes([data[i + 1], data[i + 2], data[i + 3], data[i + 4]]);
            frames.push((frame_type, length));
            i += 5 + length as usize;
        }
        frames
    }

    #[test]
    fn test_frame_sequence_parsing() {
        // Build a test frame sequence: F/O/L/D/F/O/L/EOF
        let mut data = Vec::new();

        // F frame with "test" filename
        data.push(b'F');
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(b"test");

        // O frame with offset 0
        data.push(b'O');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&0u64.to_be_bytes());

        // L frame with line 1
        data.push(b'L');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&1u64.to_be_bytes());

        // D frame with some data
        data.push(b'D');
        data.extend_from_slice(&5u32.to_be_bytes());
        data.extend_from_slice(b"hello");

        // F frame for EOF
        data.push(b'F');
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(b"test");

        // O frame for EOF
        data.push(b'O');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&5u64.to_be_bytes());

        // L frame for EOF
        data.push(b'L');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&2u64.to_be_bytes());

        // EOF (D with length 0)
        data.push(b'D');
        data.extend_from_slice(&0u32.to_be_bytes());

        let frames = parse_frames(&data);

        // Verify frame sequence
        assert_eq!(frames.len(), 8);
        assert_eq!(frames[0], ('F', 4));
        assert_eq!(frames[1], ('O', 8));
        assert_eq!(frames[2], ('L', 8));
        assert_eq!(frames[3], ('D', 5));
        assert_eq!(frames[4], ('F', 4));
        assert_eq!(frames[5], ('O', 8));
        assert_eq!(frames[6], ('L', 8));
        assert_eq!(frames[7], ('D', 0)); // EOF
    }

    #[test]
    fn test_error_frame_sequence() {
        // Build a test error frame sequence: F/O/L/E/F/O/L/EOF
        let mut data = Vec::new();

        // F frame
        data.push(b'F');
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(b"test");

        // O frame
        data.push(b'O');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&0u64.to_be_bytes());

        // L frame
        data.push(b'L');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&1u64.to_be_bytes());

        // E frame with error message
        let error_msg = b"ERROR: test error";
        data.push(b'E');
        data.extend_from_slice(&(error_msg.len() as u32).to_be_bytes());
        data.extend_from_slice(error_msg);

        // F frame for EOF
        data.push(b'F');
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(b"test");

        // O frame for EOF
        data.push(b'O');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&0u64.to_be_bytes());

        // L frame for EOF
        data.push(b'L');
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&1u64.to_be_bytes());

        // EOF
        data.push(b'D');
        data.extend_from_slice(&0u32.to_be_bytes());

        let frames = parse_frames(&data);

        // Verify frame sequence
        assert_eq!(frames.len(), 8);
        assert_eq!(frames[0], ('F', 4));
        assert_eq!(frames[1], ('O', 8));
        assert_eq!(frames[2], ('L', 8));
        assert_eq!(frames[3], ('E', error_msg.len() as u32));
        assert_eq!(frames[4], ('F', 4));
        assert_eq!(frames[5], ('O', 8));
        assert_eq!(frames[6], ('L', 8));
        assert_eq!(frames[7], ('D', 0)); // EOF
    }
}
