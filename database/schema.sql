-- schema.sql: Database schema for lottery events and aggregations

-- Event types:
--login/logout - User authentication
--view_numbers - Browse/check lottery numbers
--ticket_purchased - Core revenue event (key business metric for revenue generation)
--claim_reward - Collect winnings (metric for tracking payout obligations)

-- raw
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    amount NUMERIC(10,2) DEFAULT NULL,
    session_id INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(event_time);

-- aggregated
CREATE TABLE hourly_business_metrics (
    hour TIMESTAMP WITH TIME ZONE NOT NULL PRIMARY KEY,

    -- Revenue Velocity
    total_revenue NUMERIC(12,2) NOT NULL DEFAULT 0,
    ticket_count INTEGER NOT NULL DEFAULT 0,
    avg_ticket_value NUMERIC(8,2) NOT NULL DEFAULT 0,

    -- Payout Risk Monitoring
    total_payouts NUMERIC(12,2) NOT NULL DEFAULT 0,
    payout_events INTEGER NOT NULL DEFAULT 0,
    payout_to_revenue_ratio NUMERIC(5,4) DEFAULT NULL,

    -- User Engagement
    active_users INTEGER NOT NULL DEFAULT 0,
    new_sessions INTEGER NOT NULL DEFAULT 0,
    total_logins INTEGER NOT NULL DEFAULT 0,

    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_business_metrics_hour ON hourly_business_metrics(hour DESC);

CREATE VIEW rolling_24h_metrics AS
SELECT
    hour,
    total_revenue,
    AVG(total_revenue) OVER (
        ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) as avg_24h_revenue,
    AVG(active_users) OVER (
        ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) as avg_24h_users
FROM hourly_business_metrics;


CREATE VIEW daily_event_summary AS
SELECT
    DATE(hour) as date,
    AVG(total_revenue) as avg_revenue,
    AVG(active_users) as avg_users,
    COUNT(*) as sample_size
FROM hourly_business_metrics
WHERE hour >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(hour);
