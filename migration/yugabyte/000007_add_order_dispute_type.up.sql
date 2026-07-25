ALTER TABLE order_disputes
    ADD COLUMN IF NOT EXISTS dispute_type TEXT;
