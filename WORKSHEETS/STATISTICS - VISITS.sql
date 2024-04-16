// Create Centralized Statistics-focused database, time-travel turned off.
CREATE TRANSIENT DATABASE STATISTICS;

// Disable Time-Travel feature
ALTER DATABASE STATISTICS
SET DATA_RETENTION_TIME_IN_DAYS=0;

// Disable Time-Travel feature
ALTER SCHEMA STATISTICS.PUBLIC
SET DATA_RETENTION_TIME_IN_DAYS=0;


-- Create File Format - CSV (Visits)
CREATE FILE FORMAT STATISTICS.PUBLIC.VISITS_CSV_FORMAT
    TYPE=CSV,
    FIELD_DELIMITER=';',
    SKIP_HEADER=1,
    NULL_IF=('NULL', 'null', '"null"');




// Create Transient Table where Visit Data, coming from S3, will be copied into.
CREATE OR REPLACE TRANSIENT TABLE STATISTICS.PUBLIC.VISITS (
ACCOUNT_ID STRING, 
TITLE STRING,
PATH STRING, 
URL STRING, 
COOKIE_SYNC_ID STRING, 
PIXEL_ID STRING, 
"AS" STRING, 
CITY STRING, 
COUNTRY STRING, 
COUNTRY_CODE STRING, 
ISP STRING, 
LAT DOUBLE,  // lat and lon values are finnicky
LON DOUBLE,
ORG STRING,
QUERY STRING, 
REGION STRING, 
REGION_NAME STRING, 
STATUS STRING, 
TIME_ZONE STRING, 
ZIP STRING, 
MONTH STRING, 
__V STRING, 
CREATED_AT TIMESTAMP WITH TIME ZONE, 
UPDATED_AT TIMESTAMP WITH TIME ZONE, 
YEAR INT
);


// Disable Time-Travel feature
ALTER TABLE STATISTICS.PUBLIC.VISITS
SET DATA_RETENTION_TIME_IN_DAYS=0;



-- Create Storage Integration for AWS S3 - also adjust role and externalId in AWS policy
 CREATE STORAGE INTEGRATION STATISTICS_INTEGRATION
    TYPE=EXTERNAL_STAGE
    STORAGE_PROVIDER=S3
    ENABLED=TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::236992227954:role/visits' -- a "snowflake" dedicated IAM user is needed, in AWS, to utilize this value
    STORAGE_ALLOWED_LOCATIONS=('s3://audiencelab-visits/');


DESC STORAGE INTEGRATION STATISTICS_INTEGRATION;
DESC STORAGE INTEGRATION S3_INTEGRATION;
DESC STORAGE INTEGRATION FOUR_EYES_INTEGRATION;


// Create Visits S3 Stage
CREATE OR REPLACE STAGE STATISTICS.PUBLIC.VISITS_STAGE
    url='s3://audiencelab-visits/'
    STORAGE_INTEGRATION=STATISTICS_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='STATISTICS.PUBLIC.VISITS_CSV_FORMAT'
    );

LIST @STATISTICS.PUBLIC.VISITS_STAGE;

// Copy Statement to copy the data from S3 into Visits Table
COPY INTO STATISTICS.PUBLIC.VISITS
FROM (
SELECT
    REPLACE(T.$1, '"', '') AS ACCOUNT_ID,
    T.$2::string  AS TITLE,
    T.$3  AS PATH,
    T.$4 AS URL,
    REPLACE(T.$5::string, '"', '') AS COOKIE_SYNC_ID,
    REPLACE(T.$6::string, '"', '') AS PIXEL_ID,
    T.$7::string AS "AS",
    REPLACE(T.$8::string, '"', '') AS CITY,
    REPLACE(T.$9::string, '"', '') AS COUNTRY,
    REPLACE(T.$10::string, '"', '') AS COUNTRY_CODE,
    T.$11 AS ISP,
    TRY_CAST(REPLACE(T.$12, '"', '') AS DOUBLE) AS LAT, -- DOUBLE
    TRY_CAST(REPLACE(T.$13, '"', '') AS DOUBLE) AS LON, -- DOUBLE
    T.$14 AS ORG,
    REPLACE(T.$15, '"', '') AS QUERY,
    REPLACE(T.$16, '"', '') AS REGION,
    REPLACE(T.$17, '"', '') AS REGION_NAME,
    REPLACE(T.$18, '"', '') AS STATUS,
    REPLACE(T.$19, '"', '') as TIME_ZONE,
    REPLACE(T.$20, '"', '') AS ZIP,
    REPLACE(T.$21, '"', '') AS MONTH,
    REPLACE(T.$22, '"', '') AS __V,
    CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$23, 13, 4), '-', 
    CASE SUBSTR(T.$23, 6, 3)
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
    SUBSTR(T.$23, 10, 2), ' ', SUBSTR(T.$23, 18, 8)) ), ' ', SUBSTR(T.$23, 30, 5)) AS CREATED_AT,
    CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$24, 13, 4), '-', 
    CASE SUBSTR(T.$24, 6, 3)
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
    SUBSTR(T.$24, 10, 2), ' ', SUBSTR(T.$24, 18, 8)) ), ' ', SUBSTR(T.$24, 30, 5)) AS UPDATED_AT,
    REPLACE(T.$25, '"', '') AS YEAR
        FROM @STATISTICS.PUBLIC.VISITS_STAGE/visit/ T
)
ON_ERROR=CONTINUE;





LIST @STATISTICS.PUBLIC.VISITS_STAGE/visit;

// Validate visits table:
SELECT COUNT(*) FROM STATISTICS.PUBLIC.VISITS;
SELECT * FROM STATISTICS.PUBLIC.VISITS LIMIT 100;

// Create Role dedicated to STATISTICS Database usage
CREATE OR REPLACE ROLE STATISTICS_USER;

GRANT USAGE ON DATABASE STATISTICS TO ROLE STATISTICS_USER;
GRANT USAGE ON SCHEMA STATISTICS.PUBLIC TO ROLE STATISTICS_USER;
GRANT USAGE ON WAREHOUSE AUDIENCE_WH TO ROLE STATISTICS_USER;

GRANT SELECT ON ALL TABLES IN SCHEMA STATISTICS.PUBLIC TO ROLE STATISTICS_USER;

GRANT SELECT ON FUTURE TABLES IN SCHEMA STATISTICS.PUBLIC TO ROLE STATISTICS_USER;

GRANT ROLE STATISTICS_USER TO ROLE SYSADMIN;


















CREATE OR REPLACE TASK STATISTICS.PUBLIC.VISITS_TASK
    WAREHOUSE=DATALOADER
    SCHEDULE='USING CRON 0 0 16 * * America/Chicago'
    USER_TASK_TIMEOUT_MS=36000000
AS 
COPY INTO STATISTICS.PUBLIC.VISITS
FROM (
SELECT
    REPLACE(T.$1, '"', '') AS ACCOUNT_ID,
    T.$2::string  AS TITLE,
    T.$3  AS PATH,
    T.$4 AS URL,
    REPLACE(T.$5::string, '"', '') AS COOKIE_SYNC_ID,
    REPLACE(T.$6::string, '"', '') AS PIXEL_ID,
    T.$7::string AS "AS",
    REPLACE(T.$8::string, '"', '') AS CITY,
    REPLACE(T.$9::string, '"', '') AS COUNTRY,
    REPLACE(T.$10::string, '"', '') AS COUNTRY_CODE,
    T.$11 AS ISP,
    TRY_CAST(REPLACE(T.$12, '"', '') AS DOUBLE) AS LAT, -- DOUBLE
    TRY_CAST(REPLACE(T.$13, '"', '') AS DOUBLE) AS LON, -- DOUBLE
    T.$14 AS ORG,
    REPLACE(T.$15, '"', '') AS QUERY,
    REPLACE(T.$16, '"', '') AS REGION,
    REPLACE(T.$17, '"', '') AS REGION_NAME,
    REPLACE(T.$18, '"', '') AS STATUS,
    REPLACE(T.$19, '"', '') as TIME_ZONE,
    REPLACE(T.$20, '"', '') AS ZIP,
    REPLACE(T.$21, '"', '') AS MONTH,
    REPLACE(T.$22, '"', '') AS __V,
    CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$23, 13, 4), '-', 
    CASE SUBSTR(T.$23, 6, 3)
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
    SUBSTR(T.$23, 10, 2), ' ', SUBSTR(T.$23, 18, 8)) ), ' ', SUBSTR(T.$23, 30, 5)) AS CREATED_AT,
    CONCAT(TO_TIMESTAMP_NTZ(CONCAT(SUBSTR(T.$24, 13, 4), '-', 
    CASE SUBSTR(T.$24, 6, 3)
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
    SUBSTR(T.$24, 10, 2), ' ', SUBSTR(T.$24, 18, 8)) ), ' ', SUBSTR(T.$24, 30, 5)) AS UPDATED_AT,
    REPLACE(T.$25, '"', '') AS YEAR
        FROM @STATISTICS.PUBLIC.VISITS_STAGE/visit/ T
)
ON_ERROR=CONTINUE;





// Resume task
ALTER TASK STATISTICS.PUBLIC.VISITS_TASK RESUME;




