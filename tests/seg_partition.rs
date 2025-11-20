use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
#[ignore]
fn test_segmentation_disjoint() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_segmentation_disjoint_async().await;
    });
}

#[test]
#[ignore]
fn test_segmentation_union_complete() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        test_segmentation_union_complete_async().await;
    });
}

async fn test_segmentation_disjoint_async() {
    // Create temp directory and multiple parquet files
    let temp_dir = TempDir::new().unwrap();
    let mut file_paths = Vec::new();

    for i in 0..4 {
        let file_path = temp_dir.path().join(format!("test_{}.parquet", i));
        create_test_parquet_file(&file_path, i, i).unwrap();
        file_paths.push(file_path);
    }

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Query with 2 segments
    let files_param = file_paths
        .iter()
        .map(|p| p.to_str().unwrap())
        .collect::<Vec<_>>()
        .join(",");

    // Get segment 0 data
    let seg0_data = query_segment(&files_param, 0, 2).await;
    let seg0_ids = extract_ids(&seg0_data);

    // Get segment 1 data
    let seg1_data = query_segment(&files_param, 1, 2).await;
    let seg1_ids = extract_ids(&seg1_data);

    // Verify disjoint: no overlap between segments
    let intersection: HashSet<_> = seg0_ids.intersection(&seg1_ids).collect();
    assert!(
        intersection.is_empty(),
        "Segments should be disjoint, but have overlap: {:?}",
        intersection
    );

    // Both segments should have data
    assert!(!seg0_ids.is_empty(), "Segment 0 should have data");
    assert!(!seg1_ids.is_empty(), "Segment 1 should have data");

    drop(server_handle);
}

async fn test_segmentation_union_complete_async() {
    // Create temp directory and multiple parquet files
    let temp_dir = TempDir::new().unwrap();
    let mut file_paths = Vec::new();
    let mut all_expected_ids = HashSet::new();

    for i in 0..4 {
        let file_path = temp_dir.path().join(format!("test_{}.parquet", i));
        create_test_parquet_file(&file_path, i, i).unwrap();
        file_paths.push(file_path);
        all_expected_ids.insert(i);
    }

    // Start server
    let server_handle = start_test_server();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Query with 2 segments
    let files_param = file_paths
        .iter()
        .map(|p| p.to_str().unwrap())
        .collect::<Vec<_>>()
        .join(",");

    // Get all segment data
    let seg0_data = query_segment(&files_param, 0, 2).await;
    let seg0_ids = extract_ids(&seg0_data);

    let seg1_data = query_segment(&files_param, 1, 2).await;
    let seg1_ids = extract_ids(&seg1_data);

    // Union should equal all data
    let union: HashSet<_> = seg0_ids.union(&seg1_ids).cloned().collect();
    assert_eq!(
        union, all_expected_ids,
        "Union of segments should contain all IDs"
    );

    drop(server_handle);
}

async fn query_segment(files: &str, segment_id: usize, segment_count: usize) -> String {
    let request = format!(
        "GET /df/parquet?files={} HTTP/1.1\r\n\
         Host: localhost:18085\r\n\
         X-GP-PROTO: 0\r\n\
         X-GP-SEGMENT-ID: {}\r\n\
         X-GP-SEGMENT-COUNT: {}\r\n\
         Connection: close\r\n\
         \r\n",
        files, segment_id, segment_count
    );

    let mut stream = TcpStream::connect("127.0.0.1:18085").unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();

    // Extract body
    if let Some(body_start) = response.find("\r\n\r\n") {
        response[body_start + 4..].to_string()
    } else {
        String::new()
    }
}

fn extract_ids(csv_data: &str) -> HashSet<i32> {
    let mut ids = HashSet::new();
    for line in csv_data.lines() {
        if let Some(first_field) = line.split(',').next() {
            if let Ok(id) = first_field.trim().parse::<i32>() {
                ids.insert(id);
            }
        }
    }
    ids
}

fn create_test_parquet_file(
    path: &std::path::Path,
    id: i32,
    value: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let id_array = Int32Array::from(vec![id]);
    let value_array = Int32Array::from(vec![value]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(value_array)],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn start_test_server() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        std::env::set_var("GPFDIST_ADDR", "127.0.0.1:18085");
        let server = gpfdist_rs::Server::new("127.0.0.1:18085".to_string());
        let _ = server.run().await;
    })
}
