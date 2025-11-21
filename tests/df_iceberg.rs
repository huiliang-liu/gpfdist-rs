#[cfg(feature = "iceberg")]
mod iceberg_tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    #[test]
    #[ignore]
    fn test_iceberg_not_implemented() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_iceberg_not_implemented_async().await;
        });
    }

    async fn test_iceberg_not_implemented_async() {
        // Start server
        let server_handle = start_test_server();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Try to access iceberg endpoint
        let request = "GET /df/iceberg?path=/tmp/test HTTP/1.1\r\n\
                       Host: localhost:18083\r\n\
                       X-GP-PROTO: 0\r\n\
                       Connection: close\r\n\
                       \r\n";

        let mut stream = TcpStream::connect("127.0.0.1:18083").unwrap();
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();

        // Should get an error since iceberg is not fully implemented
        assert!(response.starts_with("HTTP/1.1 500") || response.contains("not fully implemented"));

        drop(server_handle);
    }

    fn start_test_server() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            std::env::set_var("GPFDIST_ADDR", "127.0.0.1:18083");
            let server = gpfdist_rs::Server::new("127.0.0.1:18083".to_string());
            let _ = server.run().await;
        })
    }
}

#[cfg(not(feature = "iceberg"))]
mod iceberg_disabled_tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    #[test]
    #[ignore]
    fn test_iceberg_feature_disabled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_iceberg_feature_disabled_async().await;
        });
    }

    async fn test_iceberg_feature_disabled_async() {
        // Start server
        let server_handle = start_test_server();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Try to access iceberg endpoint
        let request = "GET /df/iceberg?path=/tmp/test HTTP/1.1\r\n\
                       Host: localhost:18084\r\n\
                       X-GP-PROTO: 0\r\n\
                       Connection: close\r\n\
                       \r\n";

        let mut stream = TcpStream::connect("127.0.0.1:18084").unwrap();
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();

        // Should get 400 error
        assert!(response.starts_with("HTTP/1.1 400"));
        assert!(response.contains("Iceberg feature not enabled"));

        drop(server_handle);
    }

    fn start_test_server() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            std::env::set_var("GPFDIST_ADDR", "127.0.0.1:18084");
            let server = gpfdist_rs::Server::new("127.0.0.1:18084".to_string());
            let _ = server.run().await;
        })
    }
}
