-- schema.sql: Database schema for energy customer behavioral events and aggregations

-- Event types:
-- user_login/user_logout - Customer authentication to energy portal
-- view_tariffs - Browse available energy plans/pricing
-- tariff_switch -  Tracks customers switching between pricing tiers
-- incentive_claim - Customer claiming rebates/incentives (green energy adoption)
-- energy_consumed - Smart meter usage tracking and cost
-- bill_payment - Payment behavior tracking

-- raw events table
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    energy_consumed NUMERIC(10,3) DEFAULT NULL,     -- (kWh)
    payment_amount NUMERIC(10,2) DEFAULT NULL,      -- (EUR)
    session_id INT NOT NULL,
    tariff_type TEXT DEFAULT NULL,      -- current/new tariff plan
    channel TEXT DEFAULT NULL,          -- web_portal, mobile_app, call_center
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_customer ON events(customer_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);

-- aggregated business metrics
CREATE TABLE hourly_business_metrics (
    hour TIMESTAMP WITH TIME ZONE NOT NULL PRIMARY KEY,

    -- Customer Acquisition & Retention
    tariff_switches INTEGER NOT NULL DEFAULT 0,
    total_switch_revenue NUMERIC(12,2) NOT NULL DEFAULT 0,
    avg_switch_value NUMERIC(8,2) NOT NULL DEFAULT 0,

    -- Green Energy Adoption (Incentive Activity)
    incentive_claims INTEGER NOT NULL DEFAULT 0,
    total_incentive_payouts NUMERIC(12,2) NOT NULL DEFAULT 0,
    green_tariff_switches INTEGER NOT NULL DEFAULT 0,

    -- Customer Engagement
    active_customers INTEGER NOT NULL DEFAULT 0,
    new_sessions INTEGER NOT NULL DEFAULT 0,
    total_logins INTEGER NOT NULL DEFAULT 0,

    -- Energy Consumption Patterns
    total_energy_consumed NUMERIC(15,2) NOT NULL DEFAULT 0,  -- kWh
    avg_consumption_per_customer NUMERIC(8,2) NOT NULL DEFAULT 0,
    peak_hour_usage NUMERIC(12,2) NOT NULL DEFAULT 0,

    -- Payment Health
    total_payments NUMERIC(12,2) NOT NULL DEFAULT 0,
    payment_events INTEGER NOT NULL DEFAULT 0,
    avg_payment_amount NUMERIC(8,2) NOT NULL DEFAULT 0,

    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_business_metrics_hour ON hourly_business_metrics(hour DESC);

-- Rolling metrics for trend analysis
CREATE VIEW rolling_24h_metrics AS
SELECT
    hour,
    tariff_switches,
    total_energy_consumed,
    AVG(tariff_switches) OVER (
        ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) as avg_24h_switches,
    AVG(active_customers) OVER (
        ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) as avg_24h_customers,
    AVG(total_energy_consumed) OVER (
        ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) as avg_24h_consumption
FROM hourly_business_metrics;

-- Daily summary for business reporting
CREATE VIEW daily_energy_summary AS
SELECT
    DATE(hour) as date,
    SUM(tariff_switches) as daily_switches,
    AVG(total_energy_consumed) as avg_consumption,
    AVG(active_customers) as avg_customers,
    SUM(incentive_claims) as daily_incentive_claims,
    AVG(green_tariff_switches) as avg_green_adoptions,
    COUNT(*) as sample_size
FROM hourly_business_metrics
WHERE hour >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(hour)
ORDER BY date DESC;

-- Customer View for Journey Analytics
CREATE OR REPLACE VIEW customer_view AS
SELECT
    customer_id,

    -- Activity summary
    COUNT(*) as total_events,
    COUNT(DISTINCT session_id) as total_sessions,
    MIN(event_time) as first_seen,
    MAX(event_time) as last_seen,
    COUNT(CASE WHEN event_type = 'user_login' THEN 1 END) as logins,
    COUNT(CASE WHEN event_type = 'view_tariffs' THEN 1 END) as tariff_views,
    COUNT(CASE WHEN event_type = 'tariff_switch' THEN 1 END) as tariff_switches,
    COUNT(CASE WHEN event_type = 'bill_payment' THEN 1 END) as bill_payments,
    COUNT(CASE WHEN event_type = 'energy_consumed' THEN 1 END) as energy_reports,
    COUNT(CASE WHEN event_type = 'incentive_claim' THEN 1 END) as incentive_claims,
    COALESCE(SUM(payment_amount), 0) as total_amount_paid,
    COALESCE(SUM(energy_consumed), 0) as total_energy_consumed,
    MAX(CASE WHEN event_type = 'bill_payment' THEN event_time END) as last_payment_date,

    -- Current active tariff (most recent tariff_switch)
    (SELECT tariff_type
     FROM events e2
     WHERE e2.customer_id = e1.customer_id
       AND event_type = 'tariff_switch'
     ORDER BY event_time DESC
     LIMIT 1) as current_active_tariff,

    -- Last active tariff (second most recent tariff_switch)
    (SELECT tariff_type
     FROM events e2
     WHERE e2.customer_id = e1.customer_id
       AND event_type = 'tariff_switch'
     ORDER BY event_time DESC
     LIMIT 1 OFFSET 1) as last_active_tariff,

    -- Last tariff switch date/time
    (SELECT event_time
     FROM events e2
     WHERE e2.customer_id = e1.customer_id
       AND event_type = 'tariff_switch'
     ORDER BY event_time DESC
     LIMIT 1) as last_tariff_switch_date_time,

    -- Channels used
    ARRAY_AGG(DISTINCT channel) FILTER (WHERE channel IS NOT NULL) AS channels,

    -- Tariffs viewed or switched to
    ARRAY_AGG(DISTINCT tariff_type) FILTER (WHERE (event_type = 'view_tariffs' OR event_type = 'tariff_switch') AND tariff_type IS NOT NULL) AS tariff_types_seen

FROM events e1
GROUP BY customer_id
ORDER BY customer_id;

