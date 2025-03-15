-- Create the test table with various PostgreSQL types
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE,
    score INTEGER,
    metadata JSONB
);

-- Insert test data (100 rows)
INSERT INTO test_table (name, score, active, metadata)
SELECT 
    'User ' || i,
    (random() * 100)::INTEGER,
    i % 3 != 0,  -- Every 3rd row is inactive
    jsonb_build_object(
        'tags', jsonb_build_array('tag' || (i % 5), 'test'),
        'description', 'Description for item ' || i,
        'nested', jsonb_build_object('value', i, 'multiplier', i * 2)
    )
FROM generate_series(1, 1000) i;

-- Create another table for more complex testing
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    category TEXT,
    in_stock BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- Insert inventory test data
INSERT INTO inventory (product_name, quantity, unit_price, category, details)
SELECT
    'Product ' || i,
    (random() * 1000)::INTEGER,
    (random() * 500)::NUMERIC(10,2),
    'Category ' || (i % 5),
    jsonb_build_object(
        'dimensions', jsonb_build_object('width', i, 'height', i+5, 'depth', i+10),
        'color', (ARRAY['red','blue','green','yellow','black'])[1 + (i % 5)],
        'weight', (random() * 10)::NUMERIC(5,2),
        'features', jsonb_build_array('feature1', 'feature2', 'feature3')
    )
FROM generate_series(1, 500) i;
