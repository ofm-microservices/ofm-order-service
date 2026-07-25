ALTER TABLE order_question_snapshots
    ALTER COLUMN question_id TYPE UUID USING question_id::uuid;

ALTER TABLE order_requirement_answers
    ALTER COLUMN question_id TYPE UUID USING question_id::uuid;

ALTER TABLE order_attachments
    ADD CONSTRAINT fk_order_attachments_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_buyer_messages
    ADD CONSTRAINT fk_order_buyer_messages_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_checkout_sessions
    ADD CONSTRAINT fk_order_checkout_sessions_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_deliveries
    ADD CONSTRAINT fk_order_deliveries_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_disputes
    ADD CONSTRAINT fk_order_disputes_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_gig_snapshot
    ADD CONSTRAINT fk_order_gig_snapshot_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_question_snapshots
    ADD CONSTRAINT fk_order_question_snapshots_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_revision_requests
    ADD CONSTRAINT fk_order_revision_requests_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_delivery_files
    ADD CONSTRAINT fk_order_delivery_files_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_requirement_answers
    ADD CONSTRAINT fk_order_requirement_answers_order_id
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;

ALTER TABLE order_requirement_answers
    ADD CONSTRAINT fk_order_requirement_answers_question_snapshot
        FOREIGN KEY (order_id, question_id)
        REFERENCES order_question_snapshots(order_id, question_id)
        ON DELETE CASCADE;
