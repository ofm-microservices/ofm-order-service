CREATE TABLE IF NOT EXISTS order_delivery_files (
    order_id UUID NOT NULL,
    file_id TEXT NOT NULL,
    sort_order INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, file_id)
);
