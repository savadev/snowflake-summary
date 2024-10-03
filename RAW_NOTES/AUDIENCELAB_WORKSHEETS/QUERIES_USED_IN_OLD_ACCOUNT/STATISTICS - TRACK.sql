-- Create File Format - CSV (Visits)
CREATE FILE FORMAT STATISTICS.PUBLIC.TRACK_CSV_FORMAT
    TYPE=CSV,
    FIELD_DELIMITER=';',
    SKIP_HEADER=1,
    NULL_IF=('NULL', 'null', '"null"')




// Create Transient Table where Visit Data, coming from S3, will be copied into.
CREATE TRANSIENT TABLE STATISTICS.PUBLIC.TRACK (
  ACCOUNT_ID STRING,
  TITLE STRING,
  PATH STRING,
  URL STRING,
  ANALYTICS_ID STRING,
  COOKIE_SYNC STRING,
  PIXEL_ID STRING,
  CREATED_AT TIMESTAMP_TZ,
  UPDATED TIMESTAMP_TZ
);


// Disable Time-Travel feature
ALTER TABLE STATISTICS.PUBLIC.TRACK
SET DATA_RETENTION_TIME_IN_DAYS=0;


DESC STORAGE INTEGRATION STATISTICS_INTEGRATION;
DESC STORAGE INTEGRATION S3_INTEGRATION;
DESC STORAGE INTEGRATION FOUR_EYES_INTEGRATION;


// Create Track S3 Stage
CREATE OR REPLACE STAGE STATISTICS.PUBLIC.TRACK_STAGE
    url='s3://audiencelab-visits/'
    STORAGE_INTEGRATION=STATISTICS_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='STATISTICS.PUBLIC.TRACK_CSV_FORMAT'
    );

LIST @STATISTICS.PUBLIC.TRACK_STAGE/track;

// Copy Statement to copy the data from S3 into Track Table
COPY INTO STATISTICS.PUBLIC.TRACK
FROM (
SELECT 
REPLACE(T.$1, '"', '') AS ACCOUNT_ID,
T.$2 AS TITLE,
T.$3 AS PATH,
T.$4 AS URL,
REPLACE(T.$5, '"', '') AS ANALYTICS_ID,
REPLACE(T.$6, '"', '') AS COOKIE_SYNC,
REPLACE(T.$7, '"', '') AS PIXEL_ID,
CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$8, 13, 4), '-', 
CASE SUBSTR(T.$8, 6, 3)
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                WHEN 'Dec' THEN '12'
            END, '-',
SUBSTR(T.$8, 10, 2), ' ', SUBSTR(T.$8, 18, 8)) ), ' ', SUBSTR(T.$8, 30, 5)) AS CREATED_AT,
CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$9, 13, 4), '-', 
CASE SUBSTR(T.$9, 6, 3)
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                WHEN 'Dec' THEN '12'
            END, '-',
SUBSTR(T.$9, 10, 2), ' ', SUBSTR(T.$9, 18, 8)) ), ' ', SUBSTR(T.$9, 30, 5)) AS UPDATED_AT
-- T.$8 AS CREATED_AT,
-- T.$9 AS UPDATED_AT
FROM @STATISTICS.PUBLIC.TRACK_STAGE/track T
)
ON_ERROR=CONTINUE;



// Validate Track table:
SELECT COUNT(*) FROM STATISTICS.PUBLIC.TRACK;


SELECT * FROM STATISTICS.PUBLIC.TRACK LIMIT 1000;


GRANT SELECT ON ALL TABLES IN SCHEMA STATISTICS.PUBLIC TO ROLE STATISTICS_USER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA STATISTICS.PUBLIC TO ROLE STATISTICS_USER;
GRANT ROLE STATISTICS_USER TO ROLE SYSADMIN;






// Create task to copy s3 Track data into snowflake STATISTICS.PUBLIC.TRACK table every month, day 16, midnight CST
CREATE OR REPLACE TASK STATISTICS.PUBLIC.TRACK_TASK
    WAREHOUSE=DATALOADER
    SCHEDULE='USING CRON 0 0 15 * * America/Chicago'
    USER_TASK_TIMEOUT_MS=36000000
    AS 
    COPY INTO STATISTICS.PUBLIC.TRACK
FROM (
SELECT 
REPLACE(T.$1, '"', '') AS ACCOUNT_ID,
T.$2 AS TITLE,
T.$3 AS PATH,
T.$4 AS URL,
REPLACE(T.$5, '"', '') AS ANALYTICS_ID,
REPLACE(T.$6, '"', '') AS COOKIE_SYNC,
REPLACE(T.$7, '"', '') AS PIXEL_ID,
CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$8, 13, 4), '-', 
CASE SUBSTR(T.$8, 6, 3)
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                WHEN 'Dec' THEN '12'
            END, '-',
SUBSTR(T.$8, 10, 2), ' ', SUBSTR(T.$8, 18, 8)) ), ' ', SUBSTR(T.$8, 30, 5)) AS CREATED_AT,
CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$9, 13, 4), '-', 
CASE SUBSTR(T.$9, 6, 3)
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                WHEN 'Dec' THEN '12'
            END, '-',
SUBSTR(T.$9, 10, 2), ' ', SUBSTR(T.$9, 18, 8)) ), ' ', SUBSTR(T.$9, 30, 5)) AS UPDATED_AT
-- T.$8 AS CREATED_AT,
-- T.$9 AS UPDATED_AT
FROM @STATISTICS.PUBLIC.TRACK_STAGE/track T
);


// 3) Resume created task:

ALTER TASK STATISTICS.PUBLIC.TRACK_TASK RESUME;





