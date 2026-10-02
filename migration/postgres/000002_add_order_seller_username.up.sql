ALTER TABLE orders
ADD COLUMN IF NOT EXISTS seller_username TEXT;

UPDATE orders
SET seller_username = COALESCE(seller_username, '')
WHERE seller_username IS NULL;

ALTER TABLE order_gig_snapshot
ADD COLUMN IF NOT EXISTS seller_username TEXT;

UPDATE order_gig_snapshot
SET seller_username = COALESCE(seller_username, '')
WHERE seller_username IS NULL;
