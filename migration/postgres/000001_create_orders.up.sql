CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    saga_id UUID NOT NULL,
    buyer_id UUID NOT NULL,
    seller_id UUID NOT NULL,
    gig_id UUID NOT NULL,
    package_id UUID NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    payment_intent_id UUID,
    payment_release_id UUID,
    failure_reason TEXT,
    delivered_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    disputed_at TIMESTAMPTZ,
    buyer_response_deadline TIMESTAMPTZ,
    revision_count_used INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_gig_snapshot (
    order_id UUID PRIMARY KEY,
    gig_id UUID NOT NULL,
    gig_title TEXT NOT NULL,
    package_id UUID NOT NULL,
    package_tier TEXT NOT NULL,
    package_description TEXT NOT NULL,
    package_delivery_days INT NOT NULL,
    price_cents BIGINT NOT NULL,
    currency TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_question_snapshots (
    order_id UUID NOT NULL,
    question_id TEXT NOT NULL,
    text TEXT NOT NULL,
    type TEXT NOT NULL,
    required BOOLEAN NOT NULL,
    options_json TEXT NOT NULL,
    sort_order INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, question_id)
);

CREATE TABLE IF NOT EXISTS order_requirement_answers (
    order_id UUID NOT NULL,
    question_id TEXT NOT NULL,
    answer_value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, question_id)
);

CREATE TABLE IF NOT EXISTS order_buyer_messages (
    order_id UUID PRIMARY KEY,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_attachments (
    order_id UUID NOT NULL,
    attachment_id UUID NOT NULL,
    file_id TEXT NOT NULL,
    sort_order INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, attachment_id)
);

CREATE TABLE IF NOT EXISTS order_checkout_sessions (
    order_id UUID PRIMARY KEY,
    checkout_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_deliveries (
    order_id UUID PRIMARY KEY,
    seller_id UUID NOT NULL,
    delivery_message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_revision_requests (
    order_id UUID PRIMARY KEY,
    buyer_id UUID NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_disputes (
    order_id UUID PRIMARY KEY,
    buyer_id UUID NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_buyer_id ON orders(buyer_id);
CREATE INDEX IF NOT EXISTS idx_orders_seller_id ON orders(seller_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_saga_id ON orders(saga_id);
