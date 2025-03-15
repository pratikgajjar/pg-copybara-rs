#!/bin/bash
set -e

echo "Starting PostgreSQL containers..."
# docker compose up -d

echo "Waiting for containers to be ready..."
sleep 5  # Give some time for containers to initialize

# Connection strings for the application
SOURCE_CONN="postgresql://postgres:postgres@localhost:5434/source_db"
DEST_CONN="postgresql://postgres:postgres@localhost:5433/dest_db"

echo "=============================================="
echo "PostgreSQL containers are ready for testing!"
echo "=============================================="
echo ""
echo "Source DB: $SOURCE_CONN"
echo "Destination DB: $DEST_CONN"
echo ""
echo "Example commands to run your migration:"
echo ""
echo "# Test table migration (small batch):"
echo "cargo run -- -s test_table -d test_table --start 1 --end 100 -b 10 -c 4 \\"
echo "    --source-conn \"$SOURCE_CONN\" --dest-conn \"$DEST_CONN\""
echo ""
echo "# Test table migration (full):"
echo "cargo run -- -s test_table -d test_table --start 1 --end 1000 -b 50 -c 8 \\"
echo "    --source-conn \"$SOURCE_CONN\" --dest-conn \"$DEST_CONN\""
echo "" 
echo "# Inventory table migration:"
echo "cargo run -- -s inventory -d inventory --start 1 --end 500 -b 50 -c 4 \\"
echo "    --source-conn \"$SOURCE_CONN\" --dest-conn \"$DEST_CONN\""
echo ""
echo "To stop the containers when done:"
# echo "docker compose down"
