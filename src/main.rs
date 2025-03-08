use std::{
    collections::VecDeque,
    convert::TryInto,
    env,
    error::Error,
    fmt,
    io::{self, Read, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_recursion::async_recursion;
use bytes::{BufMut, Bytes, BytesMut};
use deadpool_postgres::{Config, Pool, Runtime};
use futures::{stream::TryStreamExt, StreamExt};
use postgres_types::{Json, Type};
use serde_json::Value;
use tokio::{
    signal,
    sync::{mpsc, Mutex},
    time,
};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Client, NoTls, RowStream, Statement};

use std::{sync::Arc, time::Instant};
use clap::Parser;
use tokio::signal;

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
    #[arg(long, env = "SOURCE_DATABASE_URL")]
    source_conn: String,

    /// Destination database connection string (defaults to source)
    #[arg(long, env = "DEST_DATABASE_URL")]
    dest_conn: Option<String>,
}

const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
const COPY_TRAILER: &[u8] = &[0xff, 0xff];

#[derive(Debug)]
struct AppError {
    message: String,
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for AppError {}

impl From<String> for AppError {
    fn from(s: String) -> Self {
        AppError { message: s }
    }
}

impl From<&str> for AppError {
    fn from(s: &str) -> Self {
        AppError {
            message: s.to_string(),
        }
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

async fn create_pool(conn_str: &str, max_size: usize) -> Result<Pool, Box<dyn Error>> {
    let mut cfg = Config::new();
    cfg.url = Some(conn_str.to_string());
    cfg.pool = deadpool_postgres::PoolConfig::new(max_size);
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(pool)
}

async fn get_table_columns(client: &Client, table: &str) -> Result<Vec<ColumnInfo>, Box<dyn Error>> {
    let rows = client
        .query(
            &format!("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1"),
            &[&table],
        )
        .await?;

    let mut columns = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        let data_type: String = row.get(1);
        let type_oid = match data_type.as_str() {
            "integer" => Type::INT4.oid(),
            "bigint" => Type::INT8.oid(),
            "jsonb" => Type::JSONB.oid(),
            "text" => Type::TEXT.oid(),
            _ => return Err(format!("Unsupported data type: {}", data_type).into()),
        };
        columns.push(ColumnInfo { name, type_oid });
    }
    Ok(columns)
}

async fn worker(
    config: JobConfig,
    mut rx: mpsc::Receiver<(i64, i64)>,
    total_rows: Arc<AtomicU64>,
) -> Result<(), Box<dyn Error>> {
    while let Some((start, end)) = rx.recv().await {
        copy_batch(&config, start, end, &total_rows).await?;
    }
    Ok(())
}

#[async_recursion]
async fn copy_batch(
    config: &JobConfig,
    start: i64,
    end: i64,
    total_rows: &Arc<AtomicU64>,
) -> Result<(), Box<dyn Error>> {
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
                time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                continue;
            }
            Err(e) => {
                if end - start == 0 {
                    log::warn!("Skipping single row {}: {}", start, e);
                    return Ok(());
                }
                let batch_size = end - start + 1;
                let new_size = (batch_size / 10).max(1);
                log::warn!("Splitting batch {}-{} into size {}", start, end, new_size);
                
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

async fn attempt_copy(config: &JobConfig, start: i64, end: i64) -> Result<u64, Box<dyn Error>> {
    let src_client = config.src_pool.get().await?;
    let dest_client = config.dest_pool.get().await?;

    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN BINARY",
        config.dest_table,
        config.dest_columns.iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\""))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut src_txn = src_client.transaction().await?;
    let cursor_name = format!("cursor_{}_{}", start, end);
    
    src_txn
        .execute(
            &format!(
                "DECLARE {} NO SCROLL CURSOR FOR SELECT * FROM {} WHERE id BETWEEN $1 AND $2",
                cursor_name, config.source_table
            ),
            &[&start, &end],
        )
        .await?;

    let stream = src_txn.query_raw(&format!("FETCH ALL FROM {}", cursor_name), &[]).await?;

    let mut dest_txn = dest_client.transaction().await?;
    let sink = dest_txn.copy_in(&copy_sql).await?;
    let mut writer = BinaryCopyInWriter::new(sink, COPY_HEADER, COPY_TRAILER);

    let mut count = 0;
    let mut stream = RowStream::new(stream);
    while let Some(row) = stream.try_next().await? {
        let mut buf = BytesMut::new();
        buf.put_u16(row.len() as u16);
        
        for (i, column) in config.dest_columns.iter().enumerate() {
            let value = row.get::<_, Option<Bytes>>(i);
            match (value, column.type_oid) {
                (None, _) => buf.put_i32(-1),
                (Some(bytes), Type::JSONB.oid()) => {
                    let json_value: Value = serde_json::from_slice(&bytes)?;
                    let mut jsonb_bytes = BytesMut::new();
                    jsonb_bytes.put_u8(1);
                    jsonb_bytes.extend_from_slice(&bytes);
                    buf.put_i32(jsonb_bytes.len() as i32);
                    buf.extend_from_slice(&jsonb_bytes);
                }
                (Some(bytes), Type::INT8.oid()) if bytes.len() == 4 => {
                    let int_val = i32::from_be_bytes(bytes[..4].try_into()?);
                    buf.put_i32(8);
                    buf.put_i64(int_val as i64);
                }
                (Some(bytes), _) => {
                    buf.put_i32(bytes.len() as i32);
                    buf.extend_from_slice(&bytes);
                }
            }
        }
        writer.write(&buf.freeze()).await?;
        count += 1;
    }

    writer.finish().await?;
    dest_txn.commit().await?;
    src_txn.commit().await?;
    Ok(count)
}

async fn report_progress(total_rows: Arc<AtomicU64>) {
    let start_time = Instant::now();
    let mut interval = time::interval(Duration::from_secs(5));
    
    loop {
        interval.tick().await;
        let rows = total_rows.load(Ordering::Relaxed);
        let duration = start_time.elapsed().as_secs_f64();
        let rate = rows as f64 / duration;
        log::info!("Progress: {} rows ({:.2} rows/sec)", rows, rate);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    
    let args: Vec<String> = env::args().collect();
    // Argument parsing omitted for brevity - use clap or similar in real code
    
    let config = JobConfig {
        src_pool: create_pool("source_conn_str", 10).await?,
        dest_pool: create_pool("dest_conn_str", 10).await?,
        source_table: "source_table".to_string(),
        dest_table: "dest_table".to_string(),
        dest_columns: vec![/* populated from DB */],
    };

    let total_rows = Arc::new(AtomicU64::new(0));
    let (tx, rx) = mpsc::channel(100);
    
    // Generate batches (example)
    for batch in generate_batches(0, 1000000, 1000) {
        tx.send(batch).await?;
    }

    let mut handles = vec![];
    for _ in 0..10 {
        let cfg = config.clone();
        let rx = rx.clone();
        let total = total_rows.clone();
        handles.push(tokio::spawn(async move {
            worker(cfg, rx, total).await
        }));
    }

    let progress = tokio::spawn(report_progress(total_rows.clone()));
    
    for handle in handles {
        handle.await??;
    }
    progress.abort();

    log::info!("Completed copying {} rows", total_rows.load(Ordering::Relaxed));
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
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
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
