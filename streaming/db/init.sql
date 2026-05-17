-- =============================================================================
-- ITSM Data Warehouse — Star Schema (PostgreSQL)
-- Plateforme Intelligente d'Analyse et de Prédiction des Incidents ITSM
-- =============================================================================

-- ======================== DIMENSION TABLES ========================

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    day_of_week     SMALLINT,       -- 0=Monday..6=Sunday
    day_of_month    SMALLINT,
    week_of_year    SMALLINT,
    month           SMALLINT,
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id     SERIAL PRIMARY KEY,
    glpi_id         INTEGER UNIQUE,
    name            VARCHAR(255) NOT NULL,
    itil_type       VARCHAR(50) DEFAULT 'incident',  -- incident, request, problem
    completename    TEXT
);

CREATE TABLE IF NOT EXISTS dim_user (
    user_id         SERIAL PRIMARY KEY,
    glpi_id         INTEGER UNIQUE,
    name_anonymized VARCHAR(255),
    role            VARCHAR(100),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_group (
    group_id        SERIAL PRIMARY KEY,
    glpi_id         INTEGER UNIQUE,
    name            VARCHAR(255) NOT NULL,
    team_type       VARCHAR(100)    -- support_l1, support_l2, admin, etc.
);

CREATE TABLE IF NOT EXISTS dim_priority (
    priority_id     SERIAL PRIMARY KEY,
    code            SMALLINT UNIQUE NOT NULL,   -- 1..5
    label           VARCHAR(50) NOT NULL,       -- Critical, High, Medium, Low, Very Low
    itil_label      VARCHAR(50)
);

-- Pre-populate priority dimension (ITIL standard)
INSERT INTO dim_priority (code, label, itil_label) VALUES
    (1, 'Very High', 'Critical'),
    (2, 'High', 'High'),
    (3, 'Medium', 'Medium'),
    (4, 'Low', 'Low'),
    (5, 'Very Low', 'Very Low')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_status (
    status_id       SERIAL PRIMARY KEY,
    code            SMALLINT UNIQUE NOT NULL,
    label           VARCHAR(50) NOT NULL
);

-- Pre-populate status dimension (GLPI standard)
INSERT INTO dim_status (code, label) VALUES
    (1, 'New'),
    (2, 'Processing (assigned)'),
    (3, 'Processing (planned)'),
    (4, 'Pending'),
    (5, 'Solved'),
    (6, 'Closed')
ON CONFLICT (code) DO NOTHING;


-- ======================== FACT TABLES ========================

CREATE TABLE IF NOT EXISTS fact_tickets (
    ticket_id           SERIAL PRIMARY KEY,
    glpi_ticket_id      VARCHAR(50) UNIQUE,
    date_creation_id    INTEGER REFERENCES dim_date(date_id),
    date_resolution_id  INTEGER REFERENCES dim_date(date_id),
    date_close_id       INTEGER REFERENCES dim_date(date_id),
    priority_id         INTEGER REFERENCES dim_priority(priority_id),
    status_id           INTEGER REFERENCES dim_status(status_id),
    category_id         INTEGER REFERENCES dim_category(category_id),
    user_id             INTEGER REFERENCES dim_user(user_id),
    group_id            INTEGER REFERENCES dim_group(group_id),
    title               TEXT,
    description         TEXT,
    urgency             SMALLINT,
    impact              SMALLINT,
    date_creation       TIMESTAMP,
    date_resolution     TIMESTAMP,
    date_close          TIMESTAMP,
    mttr_hours          REAL,               -- Mean Time To Resolve
    sla_respected       BOOLEAN,
    source              VARCHAR(20) DEFAULT 'glpi_api',   -- glpi_api | csv_batch
    ingested_at         TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_ticket_events (
    event_id        SERIAL PRIMARY KEY,
    ticket_id       INTEGER REFERENCES fact_tickets(ticket_id),
    event_type      VARCHAR(50) NOT NULL,   -- created, updated, status_change, priority_change
    event_date      TIMESTAMP NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    field_name      VARCHAR(100),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_ticket_sla (
    sla_id              SERIAL PRIMARY KEY,
    ticket_id           INTEGER REFERENCES fact_tickets(ticket_id),
    sla_deadline        TIMESTAMP,
    resolution_date     TIMESTAMP,
    sla_respected       BOOLEAN,
    delay_hours         REAL,           -- negative = resolved before deadline
    mttr_hours          REAL
);


-- ======================== ML TABLES ========================

CREATE TABLE IF NOT EXISTS ml_predictions (
    prediction_id       SERIAL PRIMARY KEY,
    ticket_id           INTEGER REFERENCES fact_tickets(ticket_id),
    predicted_priority  VARCHAR(50),
    predicted_mttr      REAL,
    confidence          REAL,
    model_version       VARCHAR(100),
    predicted_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ml_model_registry (
    model_id            SERIAL PRIMARY KEY,
    model_name          VARCHAR(255),
    model_version       VARCHAR(100),
    algorithm           VARCHAR(100),
    f1_score            REAL,
    accuracy            REAL,
    mae_mttr            REAL,
    trained_at          TIMESTAMP DEFAULT NOW(),
    model_path          TEXT,
    is_active           BOOLEAN DEFAULT FALSE
);


-- ======================== INDEXES ========================

CREATE INDEX IF NOT EXISTS idx_fact_tickets_glpi_id ON fact_tickets(glpi_ticket_id);
CREATE INDEX IF NOT EXISTS idx_fact_tickets_creation ON fact_tickets(date_creation);
CREATE INDEX IF NOT EXISTS idx_fact_tickets_priority ON fact_tickets(priority_id);
CREATE INDEX IF NOT EXISTS idx_fact_tickets_status ON fact_tickets(status_id);
CREATE INDEX IF NOT EXISTS idx_fact_tickets_category ON fact_tickets(category_id);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_events_ticket ON fact_ticket_events(ticket_id);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_sla_ticket ON fact_ticket_sla(ticket_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_ticket ON ml_predictions(ticket_id);


-- ======================== AIRFLOW DATABASE ========================
-- Create a separate database for Airflow metadata
-- (This is handled by postgres initdb scripts, but we ensure it exists)

SELECT 'ITSM Data Warehouse initialized successfully' AS status;
