# pg-copybara-rs
Blazingly fast copy data from PostgreSQL to PostgreSQL written in Rust

## Overview

This utility efficiently copies data between PostgreSQL tables using the binary COPY protocol, which is significantly faster than row-by-row operations. The application supports:

- Multi-threaded batch processing
- Automatic error retry and handling
- Graceful shutdown via signal handling
- Monitoring of progress
- Supports various PostgreSQL data types, including JSONB

## Quick Start with Docker

### Prerequisites

- Docker and Docker Compose installed
- Rust toolchain (for building the application)

### Setup Test Environment

1. Start the PostgreSQL containers:

```bash
docker-compose up -d
```

This will start two PostgreSQL instances:
- Source DB: `localhost:5432` (database: `source_db`, user: `postgres`, password: `postgres`)
- Destination DB: `localhost:5433` (database: `dest_db`, user: `postgres`, password: `postgres`)

2. Run the helper script to see usage examples:

```bash
./run_test.sh
```

### Running Data Migration

```bash
cargo run -- \
  -s test_table \
  -d test_table \
  --start 1 \
  --end 1000 \
  -b 50 \
  -c 8 \
  --source-conn "postgresql://postgres:postgres@localhost:5432/source_db" \
  --dest-conn "postgresql://postgres:postgres@localhost:5433/dest_db"
```

Where:
- `-s`: Source table name
- `-d`: Destination table name
- `--start`: Start primary key
- `--end`: End primary key
- `-b`: Batch size
- `-c`: Number of concurrent workers
- `--source-conn`: Source database connection string
- `--dest-conn`: Destination database connection string

## Development

### Building

```bash
cargo build --release
```

### Testing

After starting the Docker containers, you can test with the preloaded sample data:

```bash
# Test small batch migration
cargo run -- -s test_table -d test_table --start 1 --end 100 -b 10 -c 4 \
  --source-conn "postgresql://postgres:postgres@localhost:5432/source_db" \
  --dest-conn "postgresql://postgres:postgres@localhost:5433/dest_db"
```

### Cleaning Up

To stop and remove the Docker containers:

```bash
docker-compose down
```

Add `-v` flag to also remove the volumes (which will delete the data):

```bash
docker-compose down -v
```
