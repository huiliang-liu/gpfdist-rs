use arrow::array::RecordBatch;
use datafusion::arrow;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use futures::stream::{Stream, StreamExt};
use std::fs;
use std::pin::Pin; // [Added] Needed for directory listing

#[cfg(feature = "delta")]
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum TableType {
    Parquet,
    #[cfg(feature = "delta")]
    Delta,
    #[cfg(feature = "iceberg")]
    Iceberg,
}

#[derive(Debug, Clone)]
pub struct DFRequest {
    pub table_type: TableType,
    pub uri: String,
    pub file_list: Option<Vec<String>>,
    pub projection: Option<Vec<String>>,
    pub filter: Option<String>,
    pub limit: Option<usize>,
    pub segment_id: Option<usize>,
    pub segment_count: Option<usize>,
    pub gp_proto: u8,
    pub table_name: Option<String>,
}

pub struct DFEngine {
    ctx: SessionContext,
}

impl DFEngine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Register a parquet directory as a table
    pub async fn register_parquet_dir(
        &self,
        table_name: &str,
        path: &str,
    ) -> Result<(), DataFusionError> {
        self.ctx
            .register_parquet(table_name, path, Default::default())
            .await?;
        Ok(())
    }

    #[cfg(feature = "delta")]
    pub async fn register_delta(
        &self,
        table_name: &str,
        path: &str,
    ) -> Result<(), DataFusionError> {
        let table = deltalake::open_table(path)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Register the delta table directly using DeltaTable as a TableProvider
        let provider = Arc::new(table);
        self.ctx.register_table(table_name, provider)?;
        Ok(())
    }

    #[cfg(feature = "iceberg")]
    pub async fn register_iceberg(
        &self,
        _table_name: &str,
        _path: &str,
    ) -> Result<(), DataFusionError> {
        // Placeholder for Iceberg support
        // The iceberg-rust crate is still evolving; this is a basic placeholder
        Err(DataFusionError::NotImplemented(
            "Iceberg support is not fully implemented yet".to_string(),
        ))
    }

    /// Create a DataFrame directly from a list of parquet files.
    /// Optimized to use DataFusion's native multi-file reader.
    pub async fn dataframe_from_parquet_files(
        &self,
        files: &[String],
    ) -> Result<DataFrame, DataFusionError> {
        if files.is_empty() {
            return Err(DataFusionError::Plan("No files provided".to_string()));
        }

        // Use DataFusion's read_parquet which supports a vector of file paths.
        // This is more efficient than unioning individual dataframes.
        self.ctx
            .read_parquet(files.to_vec(), Default::default())
            .await
    }

    /// Build a SQL query string for SELECT + projection + WHERE + LIMIT
    pub fn build_sql(
        &self,
        table_name: &str,
        projection: Option<&[String]>,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> String {
        let mut sql = String::from("SELECT ");

        // Projection
        if let Some(cols) = projection {
            sql.push_str(&cols.join(", "));
        } else {
            sql.push('*');
        }

        sql.push_str(&format!(" FROM {}", table_name));

        // Filter
        if let Some(where_clause) = filter {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        // Limit
        if let Some(lim) = limit {
            sql.push_str(&format!(" LIMIT {}", lim));
        }

        sql
    }

    /// Execute the request and return raw CSV batches without gpfdist framing.
    /// The server layer is responsible for adding protocol frames (F/O/L/D/EOF).
    /// This method takes a DataFrame and returns a stream of raw CSV byte vectors,
    /// one per RecordBatch.
    pub async fn execute_csv_batches(
        &self,
        request: DFRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, String>> + Send>>, String> {
        // Get the dataframe based on table type and request parameters
        let df = self.get_dataframe(&request).await?;

        // Apply projection, filter, and limit through SQL if needed
        let df = self.apply_transformations(df, &request).await?;

        // Execute the query and get a stream of record batches
        let batches = df
            .execute_stream()
            .await
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        // Convert to CSV stream without framing
        Ok(Box::pin(self.convert_to_csv_stream(batches)))
    }

    /// Convert a stream of RecordBatch to a stream of raw CSV bytes
    fn convert_to_csv_stream(
        &self,
        mut batches: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
    ) -> impl Stream<Item = Result<Vec<u8>, String>> + Send {
        async_stream::try_stream! {
            while let Some(batch_result) = batches.next().await {
                let batch = batch_result.map_err(|e| format!("Failed to get batch: {}", e))?;

                // Convert batch to CSV in a blocking task
                let csv_data = tokio::task::spawn_blocking(move || {
                    batch_to_csv(&batch)
                })
                .await
                .map_err(|e| format!("Failed to spawn blocking task: {}", e))?
                .map_err(|e| format!("Failed to convert batch to CSV: {}", e))?;

                if !csv_data.is_empty() {
                    yield csv_data;
                }
            }
        }
    }

    /// Execute the request and return a stream of bytes in gpfdist format
    ///
    /// DEPRECATED: This method performs framing inside the engine layer.
    /// The new approach is to use `execute_csv_batches()` and perform framing
    /// at the server layer for better protocol control.
    pub async fn execute_to_gpfdist_stream(
        &self,
        request: DFRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, String>> + Send>>, String> {
        // Get the dataframe based on table type and request parameters
        let df = self.get_dataframe(&request).await?;

        // Apply projection, filter, and limit through SQL if needed
        let df = self.apply_transformations(df, &request).await?;

        // Execute the query and get a stream of record batches
        let batches = df
            .execute_stream()
            .await
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        // Convert to gpfdist format
        Ok(Box::pin(
            self.convert_to_gpfdist_stream(batches, request.gp_proto),
        ))
    }

    async fn get_dataframe(&self, request: &DFRequest) -> Result<DataFrame, String> {
        match &request.table_type {
            TableType::Parquet => {
                // 1. Get the complete list of files
                let all_files = if let Some(files) = &request.file_list {
                    // Case A: User provided explicit file list via 'files=...'
                    files.clone()
                } else {
                    // Case B: User provided a directory path via 'path=...'
                    // We must list and sort files manually to ensure consistent segmentation.
                    let path = &request.uri;
                    let mut file_list = Vec::new();

                    let entries = fs::read_dir(path)
                        .map_err(|e| format!("Failed to read directory {}: {}", path, e))?;

                    for entry in entries {
                        if let Ok(entry) = entry {
                            let path_buf = entry.path();
                            if path_buf.is_file() {
                                if let Some(path_str) = path_buf.to_str() {
                                    if path_str.ends_with(".parquet") {
                                        file_list.push(path_str.to_string());
                                    }
                                }
                            }
                        }
                    }

                    // CRITICAL: Sort the file list!
                    // Different OS/filesystems may return files in different orders.
                    // Without sorting, 'index % count' would assign different files
                    // on different segments if they see different orders, leading to data corruption.
                    file_list.sort();

                    if file_list.is_empty() {
                        return Err(format!("No parquet files found in directory: {}", path));
                    }

                    file_list
                };

                // 2. Apply segmentation logic
                // This filters the list so this segment only reads its assigned share.
                let filtered_files =
                    self.apply_segmentation(&all_files, request.segment_id, request.segment_count);

                if filtered_files.is_empty() {
                    // Case: Number of files < Number of segments.
                    // Some segments get no files. We should not error out, as that would fail the whole query.
                    // Instead, we return an empty DataFrame with the correct schema.

                    // We borrow the schema from the first available file in the full list.
                    if all_files.is_empty() {
                        return Err("No files available to infer schema".to_string());
                    }

                    // Read just one file to get the schema
                    let schema_inference_file = vec![all_files[0].clone()];
                    let df = self
                        .ctx
                        .read_parquet(schema_inference_file, Default::default())
                        .await
                        .map_err(|e| format!("Failed to read file for schema inference: {}", e))?;

                    // Apply a limit of 0 to return an empty result set with the correct schema.
                    return df
                        .limit(0, Some(0))
                        .map_err(|e| format!("Failed to create empty dataframe: {}", e));
                }

                // 3. Create DataFrame from the assigned files
                self.dataframe_from_parquet_files(&filtered_files)
                    .await
                    .map_err(|e| format!("Failed to create dataframe from files: {}", e))
            }
            #[cfg(feature = "delta")]
            TableType::Delta => {
                // Use provided table_name or fall back to default
                // Delta Lake has its own transaction log managing files.
                // Current implementation simply registers the table, which means full scan.
                // Proper segmentation for Delta requires using the low-level scan API to partition add_actions.
                let table_name = request.table_name.as_deref().unwrap_or("delta_table");
                self.register_delta(table_name, &request.uri)
                    .await
                    .map_err(|e| format!("Failed to register delta table: {}", e))?;

                self.ctx
                    .table(table_name)
                    .await
                    .map_err(|e| format!("Failed to get table: {}", e))
            }
            #[cfg(feature = "iceberg")]
            TableType::Iceberg => {
                // Iceberg support is not fully implemented yet
                // When implemented, it should use unique table names like parquet and delta:
                // let table_name = request.table_name.as_deref().unwrap_or("iceberg_table");
                // self.register_iceberg(table_name, &request.uri).await?;
                // self.ctx.table(table_name).await?;
                Err("Iceberg support is not fully implemented yet".to_string())
            }
        }
    }

    fn apply_segmentation(
        &self,
        files: &[String],
        segment_id: Option<usize>,
        segment_count: Option<usize>,
    ) -> Vec<String> {
        match (segment_id, segment_count) {
            (Some(seg_id), Some(seg_count)) if seg_count > 0 => files
                .iter()
                .enumerate()
                .filter(|(i, _)| i % seg_count == seg_id)
                .map(|(_, f)| f.clone())
                .collect(),
            _ => files.to_vec(),
        }
    }

    async fn apply_transformations(
        &self,
        df: DataFrame,
        request: &DFRequest,
    ) -> Result<DataFrame, String> {
        let mut df = df;

        // Apply projection
        if let Some(cols) = &request.projection {
            let col_refs: Vec<_> = cols.iter().map(|c| col(c)).collect();
            df = df
                .select(col_refs)
                .map_err(|e| format!("Failed to apply projection: {}", e))?;
        }

        // Apply filter
        if let Some(filter_expr) = &request.filter {
            // For simplicity, we use SQL to apply the filter
            // A more robust solution would parse the filter expression
            // For now, we'll register the dataframe as a temp table and query it
            let temp_table = "temp_filtered";
            self.ctx
                .register_table(temp_table, df.into_view())
                .map_err(|e| format!("Failed to register temp table: {}", e))?;

            let sql = self.build_sql(
                temp_table,
                request.projection.as_deref(),
                Some(filter_expr),
                request.limit,
            );

            df = self
                .ctx
                .sql(&sql)
                .await
                .map_err(|e| format!("Failed to execute filter SQL: {}", e))?;

            return Ok(df);
        }

        // Apply limit
        if let Some(lim) = request.limit {
            df = df
                .limit(0, Some(lim))
                .map_err(|e| format!("Failed to apply limit: {}", e))?;
        }

        Ok(df)
    }

    fn convert_to_gpfdist_stream(
        &self,
        mut batches: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
        gp_proto: u8,
    ) -> impl Stream<Item = Result<Vec<u8>, String>> + Send {
        async_stream::try_stream! {
            let mut line_number: u64 = 1;
            let mut byte_offset: u64 = 0;

            while let Some(batch_result) = batches.next().await {
                let batch = batch_result.map_err(|e| format!("Failed to get batch: {}", e))?;

                // Convert batch to CSV in a blocking task
                let csv_data = tokio::task::spawn_blocking(move || {
                    batch_to_csv(&batch)
                })
                .await
                .map_err(|e| format!("Failed to spawn blocking task: {}", e))?
                .map_err(|e| format!("Failed to convert batch to CSV: {}", e))?;

                if csv_data.is_empty() {
                    continue;
                }

                match gp_proto {
                    0 => {
                        // Protocol 0: raw CSV only
                        yield csv_data;
                    }
                    1 => {
                        // Protocol 1: F/O/L/D framing
                        let num_lines = csv_data.iter().filter(|&&b| b == b'\n').count() as u32;
                        let data_len = csv_data.len() as u32;

                        // F frame (File header) - sent once at the start
                        if line_number == 1 {
                            let f_frame = create_frame(b'F', 0, 0);
                            yield f_frame;
                        }

                        // O frame (Offset)
                        let o_frame = create_frame(b'O', byte_offset as u32, 0);
                        yield o_frame;

                        // L frame (Line number)
                        let l_frame = create_frame(b'L', line_number as u32, 0);
                        yield l_frame;

                        // D frame (Data)
                        let mut d_frame = create_frame(b'D', data_len, data_len);
                        d_frame.extend_from_slice(&csv_data);
                        yield d_frame;

                        line_number += num_lines as u64;
                        byte_offset += data_len as u64;
                    }
                    _ => {
                        Err(format!("Unsupported gp_proto: {}", gp_proto))?;
                    }
                }
            }

            // Send EOF frame for protocol 1
            if gp_proto == 1 {
                let eof_frame = create_frame(b'D', 0, 0);
                yield eof_frame;
            }
        }
    }
}

/// Convert a RecordBatch to CSV bytes
fn batch_to_csv(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    {
        // Use WriterBuilder to explicitly disable headers.
        // Otherwise, a header row would be written for EACH batch,
        // causing repeated column names to appear throughout the data stream.
        let mut writer = arrow_csv::WriterBuilder::new()
            .has_headers(false) // <--- Critical fix: Disable headers
            .build(&mut buf);

        writer
            .write(batch)
            .map_err(|e| format!("Failed to write CSV: {}", e))?;
    }
    Ok(buf)
}

/// Create a gpfdist protocol frame
/// Frame format: [type:1][length:4][line_or_offset:4][data...]
fn create_frame(frame_type: u8, line_or_offset: u32, length: u32) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9 + length as usize);
    frame.push(frame_type);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&line_or_offset.to_be_bytes());
    frame
}

impl Default for DFEngine {
    fn default() -> Self {
        Self::new()
    }
}
