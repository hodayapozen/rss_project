USE rss_project;

DELIMITER $$

DROP PROCEDURE IF EXISTS GetLatestNews$$

CREATE PROCEDURE GetLatestNews(IN limit_count INT)
BEGIN
    SELECT 
        s.source_name,
        s.feed_category,
        i.title,
        i.link,
        i.published_date,
        i.description,
        -- This aggregates all tag names into one string separated by commas
        GROUP_CONCAT(t.tag_name SEPARATOR ', ') AS tags
    FROM RSS_Items i
    JOIN RSS_Sources s ON i.source_id = s.source_id
    -- LEFT JOIN ensures items appear even if they have no tags
    LEFT JOIN Item_Tags it ON i.item_id = it.item_id
    LEFT JOIN RSS_Tags t ON it.tag_id = t.tag_id
    GROUP BY i.item_id
    ORDER BY i.published_date DESC
    LIMIT limit_count;
END$$

DELIMITER ;