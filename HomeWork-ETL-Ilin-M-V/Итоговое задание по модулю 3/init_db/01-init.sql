CREATE USER myapp_user WITH PASSWORD 'myapp_password';

CREATE DATABASE myapp_db OWNER myapp_user;

\c myapp_db

CREATE SCHEMA raw_data;
CREATE SCHEMA dm;

CREATE TABLE raw_data.user_sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited TEXT,
    device VARCHAR(20),
    actions TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw_data.support_tickets (
    id SERIAL PRIMARY KEY,
    ticket_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50),
    status VARCHAR(20),
    issue_type VARCHAR(50),
    messages TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT ALL ON SCHEMA raw_data TO myapp_user;
GRANT ALL ON SCHEMA dm TO myapp_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO myapp_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw_data TO myapp_user;
