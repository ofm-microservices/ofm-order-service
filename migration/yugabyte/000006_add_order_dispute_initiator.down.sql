DROP INDEX IF EXISTS idx_order_disputes_initiator_user_id;

ALTER TABLE order_disputes
    DROP COLUMN IF EXISTS initiator_role,
    DROP COLUMN IF EXISTS initiator_user_id;
