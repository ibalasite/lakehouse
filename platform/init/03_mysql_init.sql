-- ─────────────────────────────────────────────────────────────────────────────
-- 03_mysql_init.sql
-- Executed automatically by the MySQL container on first start via
-- /docker-entrypoint-initdb.d/. Creates databases, the cache schema,
-- and grants access to the lakehouse application user.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Create databases ───────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS metabase
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS lakehouse_cache
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE lakehouse_cache;

-- ── 2. Daily ticket cache ─────────────────────────────────────────────────────
-- Stores pre-aggregated ticket KPIs per day × category-sub × dimension slice.
-- Partitioned by month (RANGE on TO_DAYS) to support fast date-range pruning.
CREATE TABLE IF NOT EXISTS cache_ticket_daily (
    id                        BIGINT       NOT NULL AUTO_INCREMENT,
    date_sk                   DATE         NOT NULL COMMENT 'Partition key — calendar date of the metrics',
    catsub_id                 INT          NOT NULL COMMENT 'Category-sub dimension FK',
    prblm_source_id           INT          NULL     COMMENT 'Problem source dimension FK',
    prblm_class_id            INT          NULL     COMMENT 'Problem class dimension FK',
    prblm_perform_id          INT          NULL     COMMENT 'SLA type / performance dimension FK',
    prblm_status_id           INT          NULL     COMMENT 'Problem status dimension FK',
    total_tickets             BIGINT       NOT NULL DEFAULT 0,
    resolved_tickets          BIGINT       NOT NULL DEFAULT 0,
    one_shot_resolved         BIGINT       NOT NULL DEFAULT 0  COMMENT 'Resolved on first contact',
    complain_tickets          BIGINT       NOT NULL DEFAULT 0,
    forwarded_tickets         BIGINT       NOT NULL DEFAULT 0,
    within_sla_tickets        BIGINT       NOT NULL DEFAULT 0,
    avg_resolution_hours      DOUBLE       NULL,
    avg_response_hours        DOUBLE       NULL,
    pct_resolved              DOUBLE       NULL,
    pct_within_sla            DOUBLE       NULL,
    pct_one_shot              DOUBLE       NULL,
    updated_at                DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id, date_sk),   -- date_sk must be in PK for partitioned tables
    INDEX idx_date_sk  (date_sk),
    INDEX idx_catsub   (catsub_id),
    INDEX idx_updated  (updated_at)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (TO_DAYS(date_sk)) (
    -- 2022
    PARTITION p2022_01 VALUES LESS THAN (TO_DAYS('2022-02-01')),
    PARTITION p2022_02 VALUES LESS THAN (TO_DAYS('2022-03-01')),
    PARTITION p2022_03 VALUES LESS THAN (TO_DAYS('2022-04-01')),
    PARTITION p2022_04 VALUES LESS THAN (TO_DAYS('2022-05-01')),
    PARTITION p2022_05 VALUES LESS THAN (TO_DAYS('2022-06-01')),
    PARTITION p2022_06 VALUES LESS THAN (TO_DAYS('2022-07-01')),
    PARTITION p2022_07 VALUES LESS THAN (TO_DAYS('2022-08-01')),
    PARTITION p2022_08 VALUES LESS THAN (TO_DAYS('2022-09-01')),
    PARTITION p2022_09 VALUES LESS THAN (TO_DAYS('2022-10-01')),
    PARTITION p2022_10 VALUES LESS THAN (TO_DAYS('2022-11-01')),
    PARTITION p2022_11 VALUES LESS THAN (TO_DAYS('2022-12-01')),
    PARTITION p2022_12 VALUES LESS THAN (TO_DAYS('2023-01-01')),
    -- 2023
    PARTITION p2023_01 VALUES LESS THAN (TO_DAYS('2023-02-01')),
    PARTITION p2023_02 VALUES LESS THAN (TO_DAYS('2023-03-01')),
    PARTITION p2023_03 VALUES LESS THAN (TO_DAYS('2023-04-01')),
    PARTITION p2023_04 VALUES LESS THAN (TO_DAYS('2023-05-01')),
    PARTITION p2023_05 VALUES LESS THAN (TO_DAYS('2023-06-01')),
    PARTITION p2023_06 VALUES LESS THAN (TO_DAYS('2023-07-01')),
    PARTITION p2023_07 VALUES LESS THAN (TO_DAYS('2023-08-01')),
    PARTITION p2023_08 VALUES LESS THAN (TO_DAYS('2023-09-01')),
    PARTITION p2023_09 VALUES LESS THAN (TO_DAYS('2023-10-01')),
    PARTITION p2023_10 VALUES LESS THAN (TO_DAYS('2023-11-01')),
    PARTITION p2023_11 VALUES LESS THAN (TO_DAYS('2023-12-01')),
    PARTITION p2023_12 VALUES LESS THAN (TO_DAYS('2024-01-01')),
    -- 2024
    PARTITION p2024_01 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p2024_02 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p2024_03 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p2024_04 VALUES LESS THAN (TO_DAYS('2024-05-01')),
    PARTITION p2024_05 VALUES LESS THAN (TO_DAYS('2024-06-01')),
    PARTITION p2024_06 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p2024_07 VALUES LESS THAN (TO_DAYS('2024-08-01')),
    PARTITION p2024_08 VALUES LESS THAN (TO_DAYS('2024-09-01')),
    PARTITION p2024_09 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p2024_10 VALUES LESS THAN (TO_DAYS('2024-11-01')),
    PARTITION p2024_11 VALUES LESS THAN (TO_DAYS('2024-12-01')),
    PARTITION p2024_12 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    -- 2025
    PARTITION p2025_01 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p2025_02 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION p2025_03 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p2025_04 VALUES LESS THAN (TO_DAYS('2025-05-01')),
    PARTITION p2025_05 VALUES LESS THAN (TO_DAYS('2025-06-01')),
    PARTITION p2025_06 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p2025_07 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p2025_08 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p2025_09 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p2025_10 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p2025_11 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p2025_12 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    -- 2026
    PARTITION p2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p2026_03 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026_04 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026_07 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p2026_08 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p2026_09 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026_10 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p2026_11 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p2026_12 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    -- catch-all for future dates
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

-- ── 3. Hourly ticket cache ────────────────────────────────────────────────────
-- Same dimensions as daily but adds hour_of_day for intra-day drill-down.
-- Partitioned by month using the same date_sk column.
CREATE TABLE IF NOT EXISTS cache_ticket_hourly (
    id                        BIGINT       NOT NULL AUTO_INCREMENT,
    date_sk                   DATE         NOT NULL COMMENT 'Calendar date — partition key',
    hour_of_day               TINYINT      NOT NULL COMMENT '0-23 hour bucket',
    catsub_id                 INT          NOT NULL COMMENT 'Category-sub dimension FK',
    prblm_source_id           INT          NULL,
    prblm_class_id            INT          NULL,
    prblm_perform_id          INT          NULL COMMENT 'SLA type FK',
    prblm_status_id           INT          NULL,
    total_tickets             BIGINT       NOT NULL DEFAULT 0,
    resolved_tickets          BIGINT       NOT NULL DEFAULT 0,
    one_shot_resolved         BIGINT       NOT NULL DEFAULT 0,
    complain_tickets          BIGINT       NOT NULL DEFAULT 0,
    forwarded_tickets         BIGINT       NOT NULL DEFAULT 0,
    within_sla_tickets        BIGINT       NOT NULL DEFAULT 0,
    avg_resolution_hours      DOUBLE       NULL,
    avg_response_hours        DOUBLE       NULL,
    updated_at                DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id, date_sk),
    INDEX idx_date_hour  (date_sk, hour_of_day),
    INDEX idx_catsub     (catsub_id),
    INDEX idx_updated    (updated_at),
    CONSTRAINT chk_hour CHECK (hour_of_day BETWEEN 0 AND 23)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (TO_DAYS(date_sk)) (
    -- 2022
    PARTITION p2022_01 VALUES LESS THAN (TO_DAYS('2022-02-01')),
    PARTITION p2022_02 VALUES LESS THAN (TO_DAYS('2022-03-01')),
    PARTITION p2022_03 VALUES LESS THAN (TO_DAYS('2022-04-01')),
    PARTITION p2022_04 VALUES LESS THAN (TO_DAYS('2022-05-01')),
    PARTITION p2022_05 VALUES LESS THAN (TO_DAYS('2022-06-01')),
    PARTITION p2022_06 VALUES LESS THAN (TO_DAYS('2022-07-01')),
    PARTITION p2022_07 VALUES LESS THAN (TO_DAYS('2022-08-01')),
    PARTITION p2022_08 VALUES LESS THAN (TO_DAYS('2022-09-01')),
    PARTITION p2022_09 VALUES LESS THAN (TO_DAYS('2022-10-01')),
    PARTITION p2022_10 VALUES LESS THAN (TO_DAYS('2022-11-01')),
    PARTITION p2022_11 VALUES LESS THAN (TO_DAYS('2022-12-01')),
    PARTITION p2022_12 VALUES LESS THAN (TO_DAYS('2023-01-01')),
    -- 2023
    PARTITION p2023_01 VALUES LESS THAN (TO_DAYS('2023-02-01')),
    PARTITION p2023_02 VALUES LESS THAN (TO_DAYS('2023-03-01')),
    PARTITION p2023_03 VALUES LESS THAN (TO_DAYS('2023-04-01')),
    PARTITION p2023_04 VALUES LESS THAN (TO_DAYS('2023-05-01')),
    PARTITION p2023_05 VALUES LESS THAN (TO_DAYS('2023-06-01')),
    PARTITION p2023_06 VALUES LESS THAN (TO_DAYS('2023-07-01')),
    PARTITION p2023_07 VALUES LESS THAN (TO_DAYS('2023-08-01')),
    PARTITION p2023_08 VALUES LESS THAN (TO_DAYS('2023-09-01')),
    PARTITION p2023_09 VALUES LESS THAN (TO_DAYS('2023-10-01')),
    PARTITION p2023_10 VALUES LESS THAN (TO_DAYS('2023-11-01')),
    PARTITION p2023_11 VALUES LESS THAN (TO_DAYS('2023-12-01')),
    PARTITION p2023_12 VALUES LESS THAN (TO_DAYS('2024-01-01')),
    -- 2024
    PARTITION p2024_01 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p2024_02 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p2024_03 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p2024_04 VALUES LESS THAN (TO_DAYS('2024-05-01')),
    PARTITION p2024_05 VALUES LESS THAN (TO_DAYS('2024-06-01')),
    PARTITION p2024_06 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p2024_07 VALUES LESS THAN (TO_DAYS('2024-08-01')),
    PARTITION p2024_08 VALUES LESS THAN (TO_DAYS('2024-09-01')),
    PARTITION p2024_09 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p2024_10 VALUES LESS THAN (TO_DAYS('2024-11-01')),
    PARTITION p2024_11 VALUES LESS THAN (TO_DAYS('2024-12-01')),
    PARTITION p2024_12 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    -- 2025
    PARTITION p2025_01 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p2025_02 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION p2025_03 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p2025_04 VALUES LESS THAN (TO_DAYS('2025-05-01')),
    PARTITION p2025_05 VALUES LESS THAN (TO_DAYS('2025-06-01')),
    PARTITION p2025_06 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p2025_07 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p2025_08 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p2025_09 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p2025_10 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p2025_11 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p2025_12 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    -- 2026
    PARTITION p2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p2026_03 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026_04 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026_07 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p2026_08 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p2026_09 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026_10 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p2026_11 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p2026_12 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

-- ── 4. Grants ─────────────────────────────────────────────────────────────────
-- The lakehouse user was created by MySQL entrypoint via MYSQL_USER /
-- MYSQL_PASSWORD env vars. We only need to extend its privileges here.
GRANT ALL PRIVILEGES ON lakehouse_cache.* TO 'lakehouse'@'%';
GRANT ALL PRIVILEGES ON metabase.*         TO 'lakehouse'@'%';
FLUSH PRIVILEGES;
