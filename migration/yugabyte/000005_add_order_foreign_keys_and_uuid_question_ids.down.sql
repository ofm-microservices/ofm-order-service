ALTER TABLE order_requirement_answers
    DROP CONSTRAINT IF EXISTS fk_order_requirement_answers_question_snapshot;

ALTER TABLE order_requirement_answers
    DROP CONSTRAINT IF EXISTS fk_order_requirement_answers_order_id;

ALTER TABLE order_delivery_files
    DROP CONSTRAINT IF EXISTS fk_order_delivery_files_order_id;

ALTER TABLE order_revision_requests
    DROP CONSTRAINT IF EXISTS fk_order_revision_requests_order_id;

ALTER TABLE order_question_snapshots
    DROP CONSTRAINT IF EXISTS fk_order_question_snapshots_order_id;

ALTER TABLE order_gig_snapshot
    DROP CONSTRAINT IF EXISTS fk_order_gig_snapshot_order_id;

ALTER TABLE order_disputes
    DROP CONSTRAINT IF EXISTS fk_order_disputes_order_id;

ALTER TABLE order_deliveries
    DROP CONSTRAINT IF EXISTS fk_order_deliveries_order_id;

ALTER TABLE order_checkout_sessions
    DROP CONSTRAINT IF EXISTS fk_order_checkout_sessions_order_id;

ALTER TABLE order_buyer_messages
    DROP CONSTRAINT IF EXISTS fk_order_buyer_messages_order_id;

ALTER TABLE order_attachments
    DROP CONSTRAINT IF EXISTS fk_order_attachments_order_id;

ALTER TABLE order_requirement_answers
    ALTER COLUMN question_id TYPE TEXT USING question_id::text;

ALTER TABLE order_question_snapshots
    ALTER COLUMN question_id TYPE TEXT USING question_id::text;
