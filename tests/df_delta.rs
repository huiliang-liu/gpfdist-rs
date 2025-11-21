#[cfg(feature = "delta")]
mod delta_tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    #[ignore]
    fn test_delta_basic() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_delta_basic_async().await;
        });
    }

    async fn test_delta_basic_async() {
        // Create temp directory and delta table
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta_table");
        std::fs::create_dir_all(&table_path).unwrap();

        create_test_delta_table(&table_path).await.unwrap();

        // Start server in background
        let server_handle = start_test_server();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Make request
        let url = format!("path={}", table_path.to_str().unwrap());
        let request = format!(
            "GET /df/delta?{} HTTP/1.1\r\n\
             Host: localhost:18081\r\n\
             X-GP-PROTO: 0\r\n\
             Connection: close\r\n\
             \r\n",
            url
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

        // Should contain data
        assert!(!body.is_empty());
        assert!(body.contains("Alice") || body.contains("Bob"));

        // Cleanup
        drop(server_handle);
    }

    async fn create_test_delta_table(
        path: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["Alice", "Bob"]);

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
            std::env::set_var("GPFDIST_ADDR", "127.0.0.1:18081");
            let server = gpfdist_rs::Server::new("127.0.0.1:18081".to_string());
            let _ = server.run().await;
        })
    }
}

#[cfg(not(feature = "delta"))]
mod delta_disabled_tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    #[test]
    #[ignore]
    fn test_delta_feature_disabled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_delta_feature_disabled_async().await;
        });
    }

    async fn test_delta_feature_disabled_async() {
        // Start server
        let server_handle = start_test_server();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Try to access delta endpoint
        let request = "GET /df/delta?path=/tmp/test HTTP/1.1\r\n\
                       Host: localhost:18082\r\n\
                       X-GP-PROTO: 0\r\n\
                       Connection: close\r\n\
                       \r\n";

        let mut stream = TcpStream::connect("127.0.0.1:18082").unwrap();
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();

        // Should get 400 error
        assert!(response.starts_with("HTTP/1.1 400"));
        assert!(response.contains("Delta feature not enabled"));

        drop(server_handle);
    }

    fn start_test_server() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            std::env::set_var("GPFDIST_ADDR", "127.0.0.1:18082");
            let server = gpfdist_rs::Server::new("127.0.0.1:18082".to_string());
            let _ = server.run().await;
        })
    }
}
