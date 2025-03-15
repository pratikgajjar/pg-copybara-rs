use std::{
    array::TryFromSliceError,
    convert::TryInto,
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_recursion::async_recursion;
use bytes::{BufMut, BytesMut, Bytes};
use deadpool_postgres::{Config, CreatePoolError, Pool, PoolError, Runtime};
use futures::SinkExt;
use log;
use postgres_types::Type;
use tokio::{
    signal,
    sync::{mpsc, watch},
    time,
};
use tokio_postgres::{Client, CopyInSink, Error as TokioPostgresError, NoTls, Row};
use std::pin::Pin;

use clap::Parser;
use thiserror::Error;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Source table name
    #[arg(short, long)]
    source: String,

    /// Destination table name (defaults to source)
    #[arg(short, long)]
    dest: Option<String>,

    /// Start primary key
    #[arg(short, long)]
    start: i64,

    /// End primary key
    #[arg(short, long)]
    end: i64,

    /// Batch size
    #[arg(short, long, default_value_t = 1000)]
    batch: i64,

    /// Number of concurrent workers
    #[arg(short, long, default_value_t = 8)]
    concurrency: usize,

    /// Source database connection string
    #[arg(long)]
    source_conn: String,

    /// Destination database connection string (defaults to source)
    #[arg(long)]
    dest_conn: Option<String>,
}

// PostgreSQL binary COPY protocol constants
const PGCOPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
const PGCOPY_FLAGS: &[u8] = &[0, 0, 0, 0];
const PGCOPY_HEADER_EXT_SIZE: &[u8] = &[0, 0, 0, 0]; // Extension area size in bytes
const COPY_TRAILER: &[u8] = &[0xff, 0xff];

#[derive(Debug, Error)]
enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] TokioPostgresError),
    #[error("Connection error: {0}")]
    Connection(#[from] PoolError),
    #[error("Pool creation error: {0}")]
    PoolCreation(String),
    #[error("Invalid data type with OID {0}")]
    InvalidDataType(u32),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Array conversion error: {0}")]
    ArrayConversion(#[from] TryFromSliceError),
    #[error("Other error: {0}")]
    Other(String),
}

impl From<CreatePoolError> for AppError {
    fn from(err: CreatePoolError) -> Self {
        AppError::PoolCreation(err.to_string())
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for AppError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        AppError::Other(err.to_string())
    }
}

#[derive(Clone)]
struct JobConfig {
    src_pool: Pool,
    dest_pool: Pool,
    source_table: String,
    dest_table: String,
    dest_columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    type_oid: u32,
}

async fn create_pool(conn_str: &str, max_size: usize) -> Result<Pool, AppError> {
    let mut cfg = Config::new();
    cfg.url = Some(conn_str.to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(max_size));
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(pool)
}

async fn get_table_columns(
    client: &Client,
    table: &str,
) -> Result<Vec<ColumnInfo>, AppError> {
    let query = "
        SELECT a.attname, a.atttypid 
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        WHERE c.relname = $1 AND a.attnum > 0
    ";
    let rows = client.query(query, &[&table]).await?;
    
    rows.iter()
        .map(|row| Ok(ColumnInfo {
            name: row.try_get(0)?,
            type_oid: row.try_get(1)?,
        }))
        .collect()
}

async fn write_copy_data(
    mut sink: Pin<&mut CopyInSink<Bytes>>,
    rows: &[Row],
    columns: &[ColumnInfo],
) -> Result<(), AppError> {
    let mut buf = BytesMut::with_capacity(4096);
    
    for row in rows {
        // Number of fields
        buf.put_i16(columns.len() as i16);
        
        for (i, col) in columns.iter().enumerate() {
            // Try to get the column by name first for more reliable access
            let col_name = &col.name;
            let col_type = Type::from_oid(col.type_oid).ok_or_else(|| AppError::InvalidDataType(col.type_oid))?;
            
            // Handle each field type appropriately - check for NULL values by trying to get them first
            
            match col_type {
                Type::INT4 => {
                    if let Ok(value) = row.try_get::<_, i32>(i) {
                        buf.put_i32(4); // Length
                        buf.put_i32(value);
                    } else {
                        // If we can't get the value as expected type, use NULL
                        buf.put_i32(-1);
                    }
                },
                Type::TEXT | Type::VARCHAR => {
                    if let Ok(value) = row.try_get::<_, String>(i) {
                        let bytes = value.as_bytes();
                        buf.put_i32(bytes.len() as i32);
                        buf.put_slice(bytes);
                    } else {
                        buf.put_i32(-1);
                    }
                },
                Type::BOOL => {
                    if let Ok(value) = row.try_get::<_, bool>(i) {
                        buf.put_i32(1); // Length
                        buf.put_u8(if value { 1 } else { 0 });
                    } else {
                        buf.put_i32(-1);
                    }
                },
                Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                    // Get as string and encode as bytes
                    if let Ok(value) = row.try_get::<_, String>(i) {
                        let bytes = value.as_bytes();
                        buf.put_i32(bytes.len() as i32);
                        buf.put_slice(bytes);
                    } else {
                        buf.put_i32(-1);
                    }
                },
                Type::JSONB => {
                    if let Ok(value) = row.try_get::<_, serde_json::Value>(i) {
                        let json_string = serde_json::to_string(&value).unwrap_or_default();
                        let bytes = json_string.as_bytes();
                        buf.put_i32(bytes.len() as i32 + 1); // +1 for version byte
                        buf.put_u8(1); // JSONB version
                        buf.put_slice(bytes);
                    } else {
                        buf.put_i32(-1);
                    }
                },
                // Add other types as needed
                _ => {
                    // For unknown types, try to get as string
                    if let Ok(value) = row.try_get::<_, String>(i) {
                        let bytes = value.as_bytes();
                        buf.put_i32(bytes.len() as i32);
                        buf.put_slice(bytes);
                    } else {
                        log::warn!("Unsupported type for column {}: {:?}", col_name, col_type);
                        buf.put_i32(-1);
                    }
                }
            }
        }
    }
    
    sink.send(buf.freeze()).await?;
    
    Ok(())
}

#[async_recursion]
async fn copy_batch(
    config: &JobConfig,
    start: i64,
    end: i64,
    total_rows: &Arc<AtomicU64>,
) -> Result<(), AppError> {
    const MAX_RETRIES: usize = 3;
    let mut attempt = 0;

    loop {
        attempt += 1;
        match attempt_copy(config, start, end).await {
            Ok(count) => {
                total_rows.fetch_add(count as u64, Ordering::Relaxed);
                return Ok(());
            }
            Err(e) if attempt <= MAX_RETRIES => {
                // Log the retry
                log::warn!("Retrying batch {}-{}, attempt {}/{}: {:?}", start, end, attempt, MAX_RETRIES, e);
                time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                continue;
            }
            Err(e) => {
                if end - start == 0 {
                    log::warn!("Skipping single row {}: {:?}", start, e);
                    return Ok(());
                }
                let batch_size = end - start + 1;
                let new_size = (batch_size / 10).max(1);
                log::warn!("Splitting batch {}-{} into size {}: {:?}", start, end, new_size, e);

                let mut current = start;
                while current <= end {
                    let sub_end = (current + new_size - 1).min(end);
                    copy_batch(config, current, sub_end, total_rows).await?;
                    current = sub_end + 1;
                }
                return Ok(());
            }
        }
    }
}

async fn send_copy_header(mut sink: Pin<&mut CopyInSink<Bytes>>) -> Result<(), AppError> {
    // Create a buffer for the header
    let mut header = BytesMut::with_capacity(19); // Size of signature (11) + flags (4) + extension area size (4)
    
    // Add signature, flags, and header extension size
    header.put_slice(PGCOPY_SIGNATURE);
    header.put_slice(PGCOPY_FLAGS);
    header.put_slice(PGCOPY_HEADER_EXT_SIZE);
    
    // Send the header
    sink.send(header.freeze()).await?
;    
    Ok(())
}

async fn attempt_copy(config: &JobConfig, start: i64, end: i64) -> Result<u64, AppError> {
    let mut src_client = config.src_pool.get().await?;
    let mut dest_client = config.dest_pool.get().await?;

    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN BINARY",
        config.dest_table,
        config
            .dest_columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let src_txn = src_client.transaction().await?;
    let cursor_name = format!("cursor_{}_{}", start, end);

    // Convert i64 to i32 to match PostgreSQL's INT4 type
    let start_i32: i32 = start.try_into().map_err(|_| AppError::Other(format!("ID {} too large for i32", start)))?;
    let end_i32: i32 = end.try_into().map_err(|_| AppError::Other(format!("ID {} too large for i32", end)))?;
    
    src_txn
        .execute(
            &format!(
                "DECLARE {} NO SCROLL CURSOR FOR SELECT * FROM {} WHERE id BETWEEN $1 AND $2",
                cursor_name, config.source_table
            ),
            &[&start_i32, &end_i32],
        )
        .await?;

    let rows = src_txn
        .query(&format!("FETCH ALL FROM {}", cursor_name), &[])
        .await?;

    let dest_txn = dest_client.transaction().await?;
    let sink = dest_txn.copy_in(&copy_sql).await?;
    
    // Pin the sink
    futures::pin_mut!(sink);
    
    // Send binary COPY format header first
    send_copy_header(sink.as_mut()).await?;

    // Process in small chunks to avoid memory issues
    let chunk_size = 100;
    
    for chunk in rows.chunks(chunk_size) {
        write_copy_data(sink.as_mut(), chunk, &config.dest_columns).await?;
    }

    // Finalize
    sink.send(Bytes::from_static(COPY_TRAILER)).await?;
    
    dest_txn.commit().await.map_err(AppError::Database)?;
    src_txn.commit().await.map_err(AppError::Database)?;
    Ok(rows.len() as u64)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Cli::parse();
    let dest_table = args.dest.as_deref().unwrap_or(&args.source);
    let dest_conn = args.dest_conn.as_deref().unwrap_or(&args.source_conn);

    // Create connection pools
    let src_pool = create_pool(&args.source_conn, args.concurrency).await?;
    let dest_pool = create_pool(dest_conn, args.concurrency).await?;

    // Get destination columns
    let dest_client = dest_pool.get().await?;
    let dest_columns = get_table_columns(&dest_client, dest_table).await?;

    let config = JobConfig {
        src_pool: src_pool.clone(),
        dest_pool: dest_pool.clone(),
        source_table: args.source.clone(),
        dest_table: dest_table.to_string(),
        dest_columns,
    };

    let total_rows = Arc::new(AtomicU64::new(0));
    
    // Create a batch producer channel
    let (tx, mut rx) = mpsc::channel(100);
    
    // Create a channel for workers to notify completion
    let (done_tx, mut done_rx) = mpsc::channel(1);

    // Setup graceful shutdown
    let (shutdown_signal, shutdown_listener) = watch::channel(false);

    // Generate batches in background
    tokio::spawn({
        let tx = tx.clone();
        async move {
            for batch in generate_batches(args.start, args.end, args.batch) {
                if tx.send(batch).await.is_err() {
                    break;
                }
            }
            drop(tx); // Close producer side when all batches are sent
        }
    });

    // Worker tasks - we need to create one worker for each concurrency value
    // Unlike Go, Tokio's mpsc Receiver is not cloneable, so we need a different approach
    let mut handles = Vec::new();
    for i in 0..args.concurrency {
        let cfg = config.clone();
        let total = total_rows.clone();
        let mut shutdown = shutdown_listener.clone();
        let worker_done_tx = done_tx.clone();
        
        // Use a separate channel for each worker
        let (worker_tx, mut worker_rx) = mpsc::channel(10);
        
        // Add this worker's sender to our worker list
        let worker_sender = worker_tx.clone();
        
        // Start the worker task
        let handle = tokio::spawn(async move {
            let mut worker_done = false;
            
            loop {
                if worker_done {
                    break;
                }
                
                tokio::select! {
                    biased;
                    
                    // Handle shutdown signal
                    _ = shutdown.changed() => {
                        log::info!("Worker {} shutting down", i);
                        break;
                    },
                    
                    // Process a batch
                    result = worker_rx.recv() => {
                        match result {
                            Some((start, end)) => {
                                if let Err(e) = copy_batch(&cfg, start, end, &total).await {
                                    log::error!("Worker {}: Batch {}-{} failed: {:?}", i, start, end, e);
                                }
                            },
                            None => {
                                // Channel closed, no more batches
                                worker_done = true;
                                let _ = worker_done_tx.send(()).await;
                            }
                        }
                    }
                }
            }
        });
        
        handles.push((handle, worker_sender));
    }
    
    // Drop original sender as we've given a copy to each worker
    drop(done_tx);
    
    // Spawn a task to distribute work to all workers
    tokio::spawn({
        let worker_senders = handles.iter().map(|(_, tx)| tx.clone()).collect::<Vec<_>>();
        async move {
            let mut current_worker = 0;
            let worker_count = worker_senders.len();
            
            // Read batches from the main channel and distribute to workers
            while let Some(batch) = rx.recv().await {
                // Round-robin assignment to workers
                if let Err(e) = worker_senders[current_worker].send(batch).await {
                    log::error!("Failed to send batch to worker {}: {:?}", current_worker, e);
                }
                current_worker = (current_worker + 1) % worker_count;
            }
            
            // Close all worker channels when the main channel is closed
            for (i, tx) in worker_senders.iter().enumerate() {
                let _ = tx;
                log::debug!("Closed channel for worker {}", i);
            }
        }
    });

    // Progress reporting
    let progress_handle = tokio::spawn({
        let total_rows = total_rows.clone();
        let mut shutdown = shutdown_listener.clone();
        async move {
            let start_time = Instant::now();
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let rows = total_rows.load(Ordering::Relaxed);
                        let duration = start_time.elapsed().as_secs_f64();
                        if duration > 0.0 {
                            log::info!("Progress: {} rows ({:.2}/sec)", rows, rows as f64 / duration);
                        } else {
                            log::info!("Progress: {} rows", rows);
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
        }
    });

    // Wait for shutdown signal or all workers to finish
    tokio::select! {
        _ = signal_handler() => {
            log::info!("Shutdown signal received, waiting for workers to complete...");
            let _ = shutdown_signal.send(true);
        }
        _ = async {
            // Wait for at least one worker to signal they're done (should be the last one)
            let _ = done_rx.recv().await;
            log::info!("All batches processed!");
        } => {}
    }

    // Cleanup - abort the progress reporter
    progress_handle.abort();

    // Wait for all workers to finish (with timeout)
    if let Err(_) = tokio::time::timeout(Duration::from_secs(10), 
                                         futures::future::join_all(handles.into_iter().map(|(h, _)| h))).await {
        log::warn!("Timed out waiting for some workers to finish");
    }

    log::info!(
        "Completed copying {} rows",
        total_rows.load(Ordering::Relaxed)
    );
    Ok(())
}

fn generate_batches(start: i64, end: i64, batch_size: i64) -> Vec<(i64, i64)> {
    let mut batches = Vec::new();
    let mut current = start;
    while current <= end {
        let next = current + batch_size - 1;
        batches.push((current, next.min(end)));
        current = next + 1;
    }
    batches
}

async fn signal_handler() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler")
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
