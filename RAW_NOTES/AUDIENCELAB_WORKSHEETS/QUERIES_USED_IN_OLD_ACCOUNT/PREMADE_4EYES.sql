

-- Create and alter Transient Tables, so that they have time travel disabled, to reduce costs.
CREATE OR REPLACE TRANSIENT TABLE FOUR_EYES.PUBLIC.RAW_TOPICS (
    SHA256_LC_HEM STRING,
    TOPIC STRING,
    DATE DATE
);

CREATE OR REPLACE TRANSIENT TABLE FOUR_EYES.PUBLIC.SHA_TO_UPS (
    SHA256_LC_HEM STRING,
    UP_IDS ARRAY
);
-- Create Final (Goal) Table
    CREATE OR REPLACE TRANSIENT TABLE FOUR_EYES.PUBLIC.PREMADE_4EYES (
    DATE DATE,
    SEGMENT STRING,
    UP_ID STRING
);


ALTER TABLE FOUR_EYES.PUBLIC.RAW_TOPICS
SET DATA_RETENTION_TIME_IN_DAYS=0;

ALTER TABLE FOUR_EYES.PUBLIC.SHA_TO_UPS
SET DATA_RETENTION_TIME_IN_DAYS=0;

ALTER TABLE FOUR_EYES.PUBLIC.PREMADE_FOUR_EYES
SET DATA_RETENTION_TIME_IN_DAYS=0;



-- With the tables created, we need to set Snowpipes/Lambda functions,
-- which will trigger every time a .csv/json is uploaded to the 
-- AIGDS and hem_upid folders, in the S3 bucket.



-- For the Pipe to work, we must have prepared all 
-- of the objects needed for a normal COPY command,
-- which are:

-- 1) Stage 

-- 2) File Formats (best practice)

-- 3) Integration Object (Storage Integration)

-- 4) The Table to COPY your data into

-- Create Storage Integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION FOUR_EYES_INTEGRATION
    TYPE=EXTERNAL_STAGE
    STORAGE_PROVIDER=S3
    ENABLED=TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::236992227954:role/four-eyes-snowflake' -- a "snowflake" dedicated IAM user is needed, in AWS, to utilize this value
    STORAGE_ALLOWED_LOCATIONS=('s3://audiencelab-4eyes/');


SHOW STORAGE INTEGRATIONS;




-- Create File Format - CSV (AIGDS)
CREATE OR REPLACE FILE FORMAT FOUR_EYES.PUBLIC.FOUR_EYES_CSV_FORMAT
    TYPE=CSV,
    FIELD_DELIMITER=',',
    SKIP_HEADER=1,
    NULL_IF=('NULL', 'null')
    COMPRESSION=gzip;

-- Create File Format - JSON (HEM_UPID)
CREATE OR REPLACE FILE FORMAT FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT
    TYPE=JSON,
    COMPRESSION=gzip;

-- Create Stage - CSV (AIGDS)
CREATE OR REPLACE STAGE FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV
    url='s3://audiencelab-4eyes/AIGDS/'
    STORAGE_INTEGRATION=FOUR_EYES_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='FOUR_EYES.PUBLIC.FOUR_EYES_CSV_FORMAT'
    );

-- Create Stage - JSON (HEM_UPID)
CREATE OR REPLACE STAGE FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON
    url='s3://audiencelab-4eyes/hem_upid/'
    STORAGE_INTEGRATION=FOUR_EYES_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT'
    );



LIST @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV; -- will show the files in our bucket.
LIST @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON; -- will show the files in our bucket.



DESC STORAGE INTEGRATION FOUR_EYES_INTEGRATION;



-- Create Pipes

-- For CSV Folder
CREATE OR REPLACE PIPE FOUR_EYES.PUBLIC.CSV_PIPE
    AUTO_INGEST=TRUE
    AS 
    COPY INTO FOUR_EYES.PUBLIC.RAW_TOPICS
    FROM (
        SELECT 
        T.$1 AS "SHA256_LC_HEM",
        T.$3 AS "TOPIC", -- changed, before it was T.$2
        TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value
        FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV AS T
    );

--  For JSON Folder
CREATE OR REPLACE PIPE FOUR_EYES.PUBLIC.JSON_PIPE
    AUTO_INGEST=TRUE
    AS 
    COPY INTO FOUR_EYES.PUBLIC.SHA_TO_UPS
    FROM (
    SELECT   -- parse data from aws s3 location.
    T.$1:"SHA256_LC_HEM"::string AS "SHA256_LC_HEM",
    T.$1:"UP_IDS" AS "UP_IDS"
    FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON
    (FILE_FORMAT => FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT) T
    );

SHOW PIPES;




SELECT SYSTEM$PIPE_STATUS('FOUR_EYES.PUBLIC.JSON_PIPE');



SELECT COUNT(*) FROM FOUR_EYES.PUBLIC.RAW_TOPICS;
SELECT COUNT(*) FROM FOUR_EYES.PUBLIC.SHA_TO_UPS;
SELECT COUNT(*) FROM FOUR_EYES.PUBLIC.PREMADE_4EYES;

-- Validate Table 1 (CSV)
SELECT * FROM FOUR_EYES.PUBLIC.RAW_TOPICS LIMIT 100;



-- Validate Table 2 (JSON)
SELECT * FROM FOUR_EYES.PUBLIC.SHA_TO_UPS LIMIT 100;




SHOW PARAMETERS LIKE '%STATEMENT_TIMEOUT_IN_SECONDS%' FOR WAREHOUSE DATALOADER;



-- -- Create task to recreate the Third (Goal) Table, PREMADE_4EYES:
       CREATE OR REPLACE TASK FOUR_EYES.PUBLIC.PREMADE_4EYES_TASK
       WAREHOUSE=DATALOADER
       SCHEDULE='USING CRON 15 9 * * * America/Chicago'
       USER_TASK_TIMEOUT_MS=18000000;
 AS-- Create final table, using the data from the two initial tables:
CREATE OR REPLACE TRANSIENT TABLE "FOUR_EYES".PUBLIC.PREMADE_4EYES
    AS 
WITH STU AS (
  SELECT "UP_IDS" AS IDS FROM FOUR_EYES.PUBLIC.SHA_TO_UPS
)
SELECT
  RT.DATE AS "DATE",
  RT.TOPIC AS "SEGMENT",
  F.value::string AS up_id
FROM
  "FOUR_EYES"."PUBLIC"."RAW_TOPICS" AS RT
INNER JOIN
  "FOUR_EYES"."PUBLIC"."SHA_TO_UPS" AS SHA_TO_UPS
  ON SHA_TO_UPS."SHA256_LC_HEM" = RT."SHA256_LC_HEM"
CROSS JOIN
  LATERAL FLATTEN(input => SHA_TO_UPS.UP_IDS) F;


  ALTER TASK FOUR_EYES.PUBLIC.PREMADE_4EYES_TASK RESUME;



SHOW TASKS;




SELECT * FROM FOUR_EYES.PUBLIC.PREMADE_4EYES LIMIT 100;


SELECT * FROM AUDIENCELAB_INTERNAL_PROD.PUBLIC.PREMADE_AUDIENCES LIMIT 100;


SELECT SEGMENT, 
DATE(DATE) AS date, 
COUNT(*) AS total_records
FROM FOUR_EYES.PUBLIC.PREMADE_4EYES
GROUP BY SEGMENT, DATE(DATE)
ORDER BY SEGMENT, DATE;



GRANT USAGE ON DATABASE "FOUR_EYES" TO ROLE AUDIENCEUSER;



CREATE OR REPLACE ROLE FOUR_EYES_AGENT;


GRANT USAGE ON DATABASE "FOUR_EYES" TO ROLE FOUR_EYES_AGENT;
GRANT ALL PRIVILEGES ON SCHEMA "FOUR_EYES".PUBLIC TO ROLE FOUR_EYES_AGENT;
GRANT SELECT ON TABLE FOUR_EYES.PUBLIC.PREMADE_4EYES TO ROLE FOUR_EYES_AGENT;


GRANT USAGE ON DATABASE "FOUR_EYES" TO ROLE PIXEL_AGENT;
GRANT ALL PRIVILEGES ON SCHEMA "FOUR_EYES".PUBLIC TO ROLE PIXEL_AGENT;
GRANT SELECT ON TABLE FOUR_EYES.PUBLIC.PREMADE_4EYES TO ROLE PIXEL_AGENT;

-- very important
GRANT SELECT ON FUTURE tables in schema FOUR_EYES.PUBLIC TO ROLE PIXEL_AGENT;



GRANT ROLE FOUR_EYES_AGENT TO ROLE SYSADMIN;

GRANT ROLE FOUR_EYES_AGENT TO USER AUDIENCEUSER;


GRANT USAGE ON WAREHOUSE AUDIENCE_WH TO ROLE FOUR_EYES_AGENT;



SHOW ROLES;



USE ROLE FOUR_EYES_AGENT;

USE ROLE ACCOUNTADMIN;

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE AUDIENCE_WH;

SELECT * FROM FOUR_EYES.PUBLIC.PREMADE_4EYES LIMIT 10;





SELECT count(*) as count from (SELECT consumer.up_id FROM four_eyes.public.premade_4eyes pa INNER JOIN AUDIENCELAB_INTERNAL_PROD.PUBLIC.CONSUMER_B2C_FULL consumer ON pa.up_id=consumer.up_id WHERE consumer.id IS NOT NULL AND SEGMENT IN ('4eyes_110354','4eyes_100003') AND date > '2024-01-23');






/////// Single, dummy table:






// Create a table, using a single file from AWS S3, but only import the rows with a score of "high"
CREATE OR REPLACE TRANSIENT TABLE FOUR_EYES.PUBLIC.DUMMY_TOPICS
AS 
SELECT 
        T.$1 AS "SHA256_LC_HEM",
        T.$2 AS "TOPIC",
        T.$3 AS "SCORE",
        TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value
        FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV/dt=20240119/part-00001-be99799e-2e4a-4f3a-8686-1618290f447a-c000.csv.gz AS T
        WHERE T.$3 = 'high';



-- Validate new "Dummy Topics" table
SELECT * FROM FOUR_EYES.PUBLIC.DUMMY_TOPICS LIMIT 1000;
SELECT COUNT(*) FROM FOUR_EYES.PUBLIC.DUMMY_TOPICS; // 2123624
                                                    // 21109100


-- Populate DUMMY_TOPICS table with the content of more files (1):
    COPY INTO FOUR_EYES.PUBLIC.DUMMY_TOPICS
    FROM (
        SELECT 
        T.$1 AS "SHA256_LC_HEM",
        T.$2 AS "TOPIC",
        T.$3 AS "SCORE",
        TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value
        FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV/dt=20240119/part-00002-be99799e-2e4a-4f3a-8686-1618290f447a-c000.csv.gz AS T
    );

-- Populate DUMMY_TOPICS table with the content of more files (2):
    COPY INTO FOUR_EYES.PUBLIC.DUMMY_TOPICS
    FROM (
        SELECT 
        T.$1 AS "SHA256_LC_HEM",
        T.$2 AS "TOPIC",
        T.$3 AS "SCORE",
        TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value
        FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV/dt=20240119/part-00003-be99799e-2e4a-4f3a-8686-1618290f447a-c000.csv.gz AS T
    );

-- Populate DUMMY_TOPICS table with the content of more files (3):
    COPY INTO FOUR_EYES.PUBLIC.DUMMY_TOPICS
    FROM (
        SELECT 
        T.$1 AS "SHA256_LC_HEM",
        T.$2 AS "TOPIC",
        T.$3 AS "SCORE",
        TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value
        FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV/dt=20240119/part-00004-be99799e-2e4a-4f3a-8686-1618290f447a-c000.csv.gz AS T
    );


-- Create a new table, where the raw_topics are mapped to the SHA_TO_UPS general table (only now "raw_topics" will be "DUMMY_TOPICS", created using a single S3 file)
CREATE OR REPLACE TRANSIENT TABLE FOUR_EYES.PUBLIC.DUMMY_TOPICS_PREMADE
    AS 
WITH STU AS (
  SELECT "UP_IDS" AS IDS FROM FOUR_EYES.PUBLIC.SHA_TO_UPS
)
SELECT
  RT.DATE AS "DATE",
  RT.TOPIC AS "SEGMENT",
  F.value AS up_id
FROM
  "FOUR_EYES"."PUBLIC"."DUMMY_TOPICS" AS RT
INNER JOIN
  "FOUR_EYES"."PUBLIC"."SHA_TO_UPS" AS SHA_TO_UPS
  ON SHA_TO_UPS."SHA256_LC_HEM" = RT."SHA256_LC_HEM"
CROSS JOIN
  LATERAL FLATTEN(input => SHA_TO_UPS.UP_IDS) F;




-- Validate new DUMMY_TOPICS_PREMADE table:
SELECT * FROM "FOUR_EYES".PUBLIC.DUMMY_TOPICS_PREMADE LIMIT 10;
SELECT COUNT(*) FROM "FOUR_EYES".PUBLIC.DUMMY_TOPICS_PREMADE;





SELECT count(*) as count from (SELECT consumer.up_id FROM FOUR_EYES.PUBLIC.PREMADE_4EYES pa INNER JOIN AUDIENCELAB_INTERNAL_PROD.PUBLIC.CONSUMER_B2C_FULL consumer ON pa.up_id=consumer.up_id WHERE consumer.id IS NOT NULL AND SEGMENT IN ('4eyes_106035')  AND date >= '2023-11-01' AND date <= '2024-01-29');









// View warehouse details 

SHOW WAREHOUSES;



DESC WAREHOUSE DATALOADER;


SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' FOR TASK FOUR_EYES.PUBLIC.PREMADE_4EYES_TASK;


// Set time limit to 10 hours
ALTER WAREHOUSE DATALOADER SET STATEMENT_TIMEOUT_IN_SECONDS=36000;














----------------------------------------------------------------



// PREMADE_4EYES table now has incorrect data, has to be rebuilt;


// Step 3:  DUMP the sha to ups table and fill it with the feb 12 delivery
// Step 4: rebuild the premade_4eyes table







// Check SHA_TO_UPS table's contents:
SELECT * FROM SHA_TO_UPS LIMIT 1000;


LIST @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON/dt=20240212/;


// SHA_TO_UPS table currently has 235.8 million rows - probably data from 21 and 25th of january, and also 12 february. Needs to be have its contents truncated,
// then the rows from the dt=20240212/ folder copied into it.


// 1) Truncate SHA_TO_UPS:
TRUNCATE TABLE FOUR_EYES.PUBLIC.SHA_TO_UPS;


// 2) Copy Data of dt=20240212 into SHA_TO_UPS table
   // COPY INTO FOUR_EYES.PUBLIC.SHA_TO_UPS
   // FROM (
   // SELECT   -- parse data from aws s3 location.
   // T.$1:"SHA256_LC_HEM"::string AS "SHA256_LC_HEM",
  //  T.$1:"UP_IDs" AS "UP_IDS"
   // FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON/dt=20240212/
   // (FILE_FORMAT => FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT) T
  //  );


// 2.1) Copy Data of dt=20240121/
  //  COPY INTO FOUR_EYES.PUBLIC.SHA_TO_UPS
   // FROM (
  //  SELECT   -- parse data from aws s3 location.
  //  T.$1:"SHA256_LC_HEM"::string AS "SHA256_LC_HEM",
  //  T.$1:"UP_IDS" AS "UP_IDS"
  //  FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON/dt=20240121/
   // (FILE_FORMAT => FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT) T
   // );



// 2.2) Copy Data of dt=20240125/
 //   COPY INTO FOUR_EYES.PUBLIC.SHA_TO_UPS
  //  FROM (
  //  SELECT   -- parse data from aws s3 location.
  //  T.$1:"SHA256_LC_HEM"::string AS "SHA256_LC_HEM",
   // T.$1:"UP_IDS" AS "UP_IDS"
   // FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON/dt=20240125/
   // (FILE_FORMAT => FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT) T
   // );



// 2.3) Copy Data of dt=20240213/
   COPY INTO FOUR_EYES.PUBLIC.SHA_TO_UPS
    FROM (
    SELECT   -- parse data from aws s3 location.
    T.$1:"SHA256_LC_HEM"::string AS "SHA256_LC_HEM",
    T.$1:"UP_IDS" AS "UP_IDS"
    FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON/dt=20240213/
    (FILE_FORMAT => FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT) T
      );
   


// 3) Validate SHA_TO_UPS table:
SELECT * FROM FOUR_EYES.PUBLIC.SHA_TO_UPS LIMIT 1000;
SELECT COUNT(*) FROM FOUR_EYES.PUBLIC.SHA_TO_UPS;



SELECT * FROM FOUR_EYES.PUBLIC.RAW_TOPICS LIMIT 100;


// 4) Recreate PREMADE_4EYES table, using updated SHA_TO_UPS table:
CREATE OR REPLACE TRANSIENT TABLE "FOUR_EYES".PUBLIC.PREMADE_4EYES
    AS 
WITH STU AS (
  SELECT "UP_IDS" AS IDS FROM FOUR_EYES.PUBLIC.SHA_TO_UPS
)
SELECT
  RT.DATE AS "DATE",
  RT.TOPIC AS "SEGMENT",
  F.value::string AS up_id
FROM
  "FOUR_EYES"."PUBLIC"."RAW_TOPICS" AS RT
INNER JOIN
  "FOUR_EYES"."PUBLIC"."SHA_TO_UPS" AS SHA_TO_UPS
  ON SHA_TO_UPS."SHA256_LC_HEM" = RT."SHA256_LC_HEM"
CROSS JOIN
  LATERAL FLATTEN(input => SHA_TO_UPS.UP_IDS) F;














  

