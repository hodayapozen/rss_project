USE rss_project;

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