ALTER TABLE order_disputes
    ADD COLUMN IF NOT EXISTS initiator_user_id UUID,
    ADD COLUMN IF NOT EXISTS initiator_role TEXT NOT NULL DEFAULT 'buyer';

UPDATE order_disputes
SET initiator_user_id = buyer_id
WHERE initiator_user_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_order_disputes_initiator_user_id
    ON order_disputes(initiator_user_id);
