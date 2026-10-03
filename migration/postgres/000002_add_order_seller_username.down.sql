ALTER TABLE order_gig_snapshot
DROP COLUMN IF EXISTS seller_username;

ALTER TABLE orders
DROP COLUMN IF EXISTS seller_username;
