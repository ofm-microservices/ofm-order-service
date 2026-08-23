CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    operation TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_aggregate
    ON outbox_events (aggregate_type, aggregate_id, created_at);

CREATE OR REPLACE FUNCTION capture_order_outbox_event() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE row_data JSONB; operation_value TEXT := CASE TG_OP WHEN 'INSERT' THEN 'created' WHEN 'DELETE' THEN 'deactivated' ELSE 'updated' END;
BEGIN
    row_data := CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
    INSERT INTO outbox_events(event_id, aggregate_type, aggregate_id, event_type, operation, payload)
    VALUES (md5(clock_timestamp()::TEXT || random()::TEXT)::UUID, 'orders', COALESCE(row_data->>'order_id', row_data->>'id'), 'order-service.orders.changed', operation_value, row_data);
    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$$;

DROP TRIGGER IF EXISTS orders_outbox ON orders;
CREATE TRIGGER orders_outbox AFTER INSERT OR UPDATE OR DELETE ON orders FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_gig_snapshot_outbox ON order_gig_snapshot;
CREATE TRIGGER order_gig_snapshot_outbox AFTER INSERT OR UPDATE OR DELETE ON order_gig_snapshot FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_question_snapshots_outbox ON order_question_snapshots;
CREATE TRIGGER order_question_snapshots_outbox AFTER INSERT OR UPDATE OR DELETE ON order_question_snapshots FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_requirement_answers_outbox ON order_requirement_answers;
CREATE TRIGGER order_requirement_answers_outbox AFTER INSERT OR UPDATE OR DELETE ON order_requirement_answers FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_buyer_messages_outbox ON order_buyer_messages;
CREATE TRIGGER order_buyer_messages_outbox AFTER INSERT OR UPDATE OR DELETE ON order_buyer_messages FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_attachments_outbox ON order_attachments;
CREATE TRIGGER order_attachments_outbox AFTER INSERT OR UPDATE OR DELETE ON order_attachments FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_checkout_sessions_outbox ON order_checkout_sessions;
CREATE TRIGGER order_checkout_sessions_outbox AFTER INSERT OR UPDATE OR DELETE ON order_checkout_sessions FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_deliveries_outbox ON order_deliveries;
CREATE TRIGGER order_deliveries_outbox AFTER INSERT OR UPDATE OR DELETE ON order_deliveries FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_revision_requests_outbox ON order_revision_requests;
CREATE TRIGGER order_revision_requests_outbox AFTER INSERT OR UPDATE OR DELETE ON order_revision_requests FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_disputes_outbox ON order_disputes;
CREATE TRIGGER order_disputes_outbox AFTER INSERT OR UPDATE OR DELETE ON order_disputes FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
DROP TRIGGER IF EXISTS order_delivery_files_outbox ON order_delivery_files;
CREATE TRIGGER order_delivery_files_outbox AFTER INSERT OR UPDATE OR DELETE ON order_delivery_files FOR EACH ROW EXECUTE FUNCTION capture_order_outbox_event();
