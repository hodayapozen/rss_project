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

-- Disable foreign keys check to allow safe dropping
SET FOREIGN_KEY_CHECKS = 0;

-- ==========================================================
-- 1. DROP EXISTING TABLES (Order matters!)
-- ==========================================================
-- DROP TABLE IF EXISTS Item_Tags;
-- DROP TABLE IF EXISTS RSS_Items;
-- DROP TABLE IF EXISTS RSS_Tags;
-- DROP TABLE IF EXISTS RSS_Sources;
-- DROP TABLE IF EXISTS processed_raw_items;

-- ==========================================================
-- 2. CREATE TABLES
-- ==========================================================

-- 1. Sources: Defined by Source Name + Feed Category
CREATE TABLE IF NOT EXISTS RSS_Sources (
    source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL,
    feed_category VARCHAR(255) NOT NULL,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_feed (source_name, feed_category)
);

-- 2. Items: The main headlines
CREATE TABLE IF NOT EXISTS RSS_Items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    raw_guid VARCHAR(512) UNIQUE,
    source_id INT,
    title VARCHAR(512),
    link VARCHAR(2048),
    published_date DATETIME,
    description TEXT,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES RSS_Sources(source_id)
);

-- 3. Tags: Unique list of tags from the JSON
CREATE TABLE IF NOT EXISTS RSS_Tags (
    tag_id INT AUTO_INCREMENT PRIMARY KEY,
    tag_name VARCHAR(255) UNIQUE NOT NULL,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Many-to-Many Link: Connecting Items to Tags
CREATE TABLE IF NOT EXISTS Item_Tags (
    item_id INT,
    tag_id INT,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (item_id, tag_id),
    FOREIGN KEY (item_id) REFERENCES RSS_Items(item_id),
    FOREIGN KEY (tag_id) REFERENCES RSS_Tags(tag_id)
);

-- 5. Processing Log
CREATE TABLE IF NOT EXISTS processed_raw_items (
    raw_item_id VARCHAR(512) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Re-enable foreign keys
SET FOREIGN_KEY_CHECKS = 1;

