#[cfg(feature = "delta")]
mod session_tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    const TEST_SERVER_ADDR: &str = "127.0.0.1:18085";

    #[test]
    #[ignore]
    fn test_session_single_read() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_session_single_read_async().await;
        });
    }

    async fn test_session_single_read_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        // Make first request with session headers
        let url = format!("path={}", table_path.to_str().unwrap());
        let response1 = make_request_with_session(&url, "xid1", "cid1", "sn1", 1).await;

        // Validate first response contains data
        assert!(response1.starts_with("HTTP/1.1 200 OK"));
        let body1 = extract_body(&response1);
        assert!(!body1.is_empty(), "First request should contain data");
        assert!(body1.contains("Alice") || body1.contains("Bob"));

        // Make second request with same session headers (should return EOF immediately)
        let response2 = make_request_with_session(&url, "xid1", "cid1", "sn1", 1).await;

        // Validate second response is minimal (just EOF)
        assert!(response2.starts_with("HTTP/1.1 200 OK"));
        let body2 = extract_body(&response2);
        // Should be minimal - just protocol frames, no actual CSV data
        assert!(
            body2.len() < body1.len() / 2,
            "Second request should have minimal response"
        );

        // Cleanup
        drop(server_handle);
    }

    #[test]
    #[ignore]
    fn test_session_multiple_segments_same_session() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_session_multiple_segments_same_session_async().await;
        });
    }

    async fn test_session_multiple_segments_same_session_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        let url = format!("path={}", table_path.to_str().unwrap());

        // Simulate 3 segments making requests with the SAME session key
        // The first one should get data, the others should get minimal EOF
        let mut handles = vec![];

        for seg_id in 0..3 {
            let url_clone = url.clone();
            let handle = tokio::spawn(async move {
                // Add small delays to stagger requests
                sleep(Duration::from_millis(seg_id as u64 * 100)).await;
                make_request_with_session(&url_clone, "xid2", "cid2", "sn2", 1).await
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        let mut responses = vec![];
        for handle in handles {
            responses.push(handle.await.unwrap());
        }

        // All should return 200 OK
        for response in &responses {
            assert!(response.starts_with("HTTP/1.1 200 OK"));
        }

        // At least one should have full data
        let bodies: Vec<String> = responses.iter().map(|r| extract_body(r)).collect();
        let max_len = bodies.iter().map(|b| b.len()).max().unwrap();
        let min_len = bodies.iter().map(|b| b.len()).min().unwrap();

        // The first request should have gotten full data
        assert!(max_len > 50, "At least one response should have data");

        // Later requests should have minimal data (just EOF)
        // Some responses should be much smaller than the full data response
        assert!(
            min_len < max_len / 2,
            "Some responses should be minimal (cached)"
        );

        // Cleanup
        drop(server_handle);
    }

    #[test]
    #[ignore]
    fn test_session_different_sessions_get_data() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_session_different_sessions_get_data_async().await;
        });
    }

    async fn test_session_different_sessions_get_data_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        let url = format!("path={}", table_path.to_str().unwrap());

        // Make requests with DIFFERENT session keys
        let response1 = make_request_with_session(&url, "xid3", "cid3", "sn3", 1).await;
        let response2 = make_request_with_session(&url, "xid4", "cid4", "sn4", 1).await;
        let response3 = make_request_with_session(&url, "xid5", "cid5", "sn5", 1).await;

        // All should return 200 OK with data
        assert!(response1.starts_with("HTTP/1.1 200 OK"));
        assert!(response2.starts_with("HTTP/1.1 200 OK"));
        assert!(response3.starts_with("HTTP/1.1 200 OK"));

        let body1 = extract_body(&response1);
        let body2 = extract_body(&response2);
        let body3 = extract_body(&response3);

        // All should have full data (different sessions)
        assert!(!body1.is_empty());
        assert!(!body2.is_empty());
        assert!(!body3.is_empty());

        // All should be similar in size (all got full data)
        assert!(
            body1.len() > 50 && body2.len() > 50 && body3.len() > 50,
            "All different sessions should get full data"
        );

        // Cleanup
        drop(server_handle);
    }

    #[test]
    #[ignore]
    fn test_no_session_headers_no_caching() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_no_session_headers_no_caching_async().await;
        });
    }

    async fn test_no_session_headers_no_caching_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        sleep(Duration::from_millis(500)).await;

        let url = format!("path={}", table_path.to_str().unwrap());

        // Make requests WITHOUT session headers
        let response1 = make_request_no_session(&url, 1).await;
        let response2 = make_request_no_session(&url, 1).await;

        // Both should return 200 OK with full data
        assert!(response1.starts_with("HTTP/1.1 200 OK"));
        assert!(response2.starts_with("HTTP/1.1 200 OK"));

        let body1 = extract_body(&response1);
        let body2 = extract_body(&response2);

        // Both should have full data (no session caching)
        assert!(!body1.is_empty());
        assert!(!body2.is_empty());
        assert!(
            body1.len() > 50 && body2.len() > 50,
            "Both requests without session headers should get full data"
        );

        // Cleanup
        drop(server_handle);
    }

    async fn make_request_with_session(
        url: &str,
        xid: &str,
        cid: &str,
        sn: &str,
        proto: u8,
    ) -> String {
        let request = format!(
            "GET /df/delta?{} HTTP/1.1\r\n\
             Host: localhost:18085\r\n\
             X-GP-PROTO: {}\r\n\
             X-GP-XID: {}\r\n\
             X-GP-CID: {}\r\n\
             X-GP-SN: {}\r\n\
             Connection: close\r\n\
             \r\n",
            url, proto, xid, cid, sn
        );

        let mut stream = TcpStream::connect(TEST_SERVER_ADDR).unwrap();
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    async fn make_request_no_session(url: &str, proto: u8) -> String {
        let request = format!(
            "GET /df/delta?{} HTTP/1.1\r\n\
             Host: localhost:18085\r\n\
             X-GP-PROTO: {}\r\n\
             Connection: close\r\n\
             \r\n",
            url, proto
        );

        let mut stream = TcpStream::connect(TEST_SERVER_ADDR).unwrap();
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    fn extract_body(response: &str) -> String {
        if let Some(pos) = response.find("\r\n\r\n") {
            response[pos + 4..].to_string()
        } else {
            String::new()
        }
    }

    async fn create_test_delta_table(
        path: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

        // Use DeltaOps to create and write to delta table
        let ops = deltalake::DeltaOps::try_from_uri(path.to_str().unwrap()).await?;
        ops.write(vec![batch])
            .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
            .await?;

        Ok(())
    }

    fn start_test_server() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            std::env::set_var("GPFDIST_ADDR", TEST_SERVER_ADDR);
            let server = gpfdist_rs::Server::new(TEST_SERVER_ADDR.to_string());
            let _ = server.run().await;
        })
    }
}

#[cfg(not(feature = "delta"))]
mod session_disabled_tests {
    // Session management tests require delta feature for testing
    // If delta is not enabled, we can't test session management with delta tables
    #[test]
    fn test_session_requires_delta_feature() {
        // This is a placeholder test
        assert!(true);
    }
}
