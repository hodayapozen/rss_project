USE rss_project;

CREATE TABLE IF NOT EXISTS rss_raw_items (
    id VARCHAR(512) PRIMARY KEY,
    source VARCHAR(50),
    category VARCHAR(255),
    title VARCHAR(512),
    link VARCHAR(2048),
    published_date DATETIME,
    description TEXT,
    tags JSON,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

