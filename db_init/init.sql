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


DELIMITER $$

DROP PROCEDURE IF EXISTS NormalizeRSSData$$

CREATE PROCEDURE NormalizeRSSData()
BEGIN
    -- ==========================================================
    -- Step 1: Insert Sources
    -- ==========================================================
    INSERT IGNORE INTO RSS_Sources (source_name, feed_category)
    SELECT DISTINCT source, category 
    FROM rss_raw_items 
    WHERE id NOT IN (SELECT raw_item_id FROM processed_raw_items);

    -- ==========================================================
    -- Step 2: Insert Items
    -- ==========================================================
    -- Note: insert_date is filled automatically
    INSERT INTO RSS_Items (raw_guid, source_id, title, link, published_date, description)
    SELECT 
        r.id,
        s.source_id,
        r.title,
        r.link,
        r.published_date,
        r.description
    FROM rss_raw_items r
    JOIN RSS_Sources s 
      ON r.source = s.source_name 
      AND r.category = s.feed_category
    WHERE r.id NOT IN (SELECT raw_item_id FROM processed_raw_items);

    -- ==========================================================
    -- Step 3: Handle Tags (Robust Parsing)
    -- ==========================================================
    -- A. Insert unique tags
    INSERT IGNORE INTO RSS_Tags (tag_name)
    SELECT DISTINCT TRIM(BOTH '"' FROM j.tag_name)
    FROM rss_raw_items r,
    JSON_TABLE(
        JSON_UNQUOTE(r.tags), 
        "$[*]" COLUMNS (tag_name VARCHAR(255) PATH "$")
    ) j
    WHERE r.tags IS NOT NULL 
      AND JSON_LENGTH(r.tags) > 0
      AND r.id NOT IN (SELECT raw_item_id FROM processed_raw_items);

    -- B. Link Items to Tags
    INSERT IGNORE INTO Item_Tags (item_id, tag_id)
    SELECT DISTINCT
        i.item_id,
        t.tag_id
    FROM rss_raw_items r
    JOIN RSS_Items i ON r.id = i.raw_guid
    JOIN JSON_TABLE(
        JSON_UNQUOTE(r.tags), 
        "$[*]" COLUMNS (tag_name VARCHAR(255) PATH "$")
    ) j
    JOIN RSS_Tags t ON TRIM(BOTH '"' FROM j.tag_name) = t.tag_name
    WHERE r.tags IS NOT NULL 
      AND JSON_LENGTH(r.tags) > 0
      AND r.id NOT IN (SELECT raw_item_id FROM processed_raw_items);

    -- ==========================================================
    -- Step 4: Mark as Processed
    -- ==========================================================
    INSERT IGNORE INTO processed_raw_items (raw_item_id)
    SELECT id 
    FROM rss_raw_items 
    WHERE id NOT IN (SELECT raw_item_id FROM processed_raw_items);

    -- Log completion
    SELECT CONCAT('Batch processing completed at ', NOW()) AS Status;

END$$

DELIMITER ;
