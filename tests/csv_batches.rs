use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;
use futures::StreamExt;
use gpfdist_rs::df_engine::{DFEngine, DFRequest, TableType};

#[tokio::test]
async fn test_execute_csv_batches() {
    // Create temp directory and parquet file
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.parquet");
    
    // Create test parquet file
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    
    let batch = RecordBatch::try_new(
        schema.clone(), 
        vec![Arc::new(id_array), Arc::new(name_array)]
    ).unwrap();
    
    let file = File::create(&file_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    
    // Test execute_csv_batches
    let engine = DFEngine::new();
    let request = DFRequest {
        table_type: TableType::Parquet,
        uri: file_path.to_str().unwrap().to_string(),
        file_list: Some(vec![file_path.to_str().unwrap().to_string()]),
        projection: None,
        filter: None,
        limit: None,
        segment_id: None,
        segment_count: None,
        gp_proto: 1,
    };
    
    let mut stream = engine.execute_csv_batches(request).await.unwrap();
    
    let mut batch_count = 0;
    let mut total_bytes = 0;
    
    while let Some(result) = stream.next().await {
        match result {
            Ok(csv_bytes) => {
                batch_count += 1;
                total_bytes += csv_bytes.len();
                
                // Verify CSV content
                let csv_str = String::from_utf8_lossy(&csv_bytes);
                assert!(csv_str.contains("Alice") || csv_str.contains("Bob") || csv_str.contains("Charlie"));
            }
            Err(e) => {
                panic!("Stream error: {}", e);
            }
        }
    }
    
    assert!(batch_count > 0, "Should have at least one batch");
    assert!(total_bytes > 0, "Should have data bytes");
}
