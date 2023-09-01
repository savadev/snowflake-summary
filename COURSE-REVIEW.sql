-- REVIEWING SYNTAX, TIPS AND TRICKS OF ENTIRE SECOUND COURSE -- 



-- MODULE 1 --

-- Warehouse means a group of nodes 
-- and clusters which  helps to process the data.



-- Creating a warehouse, to run queries:
CREATE OR REPLACE WAREHOUSE audiencelab_main with
warehouse_size='X-SMALL'
auto_suspend = 180
auto_resume = true
initially_suspended=true;

-- Warehouse Sizes. For each increase in size, the compute costs per hour (credits) are doubled.

-- XSMALL , 'X-SMALL'  1
-- SMALL    2
-- MEDIUM   4
-- LARGE    8
-- XLARGE , 'X-LARGE'   16
-- XXLARGE , X2LARGE , '2X-LARGE'   32
-- XXXLARGE , X3LARGE , '3X-LARGE'  64
-- X4LARGE , '4X-LARGE'     128

-- Force resume a warehouse. "OPERATE" privileges are needed to run this query.
ALTER WAREHOUSE audiencelab_main RESUME;

-- Force suspend a warehouse. "OPERATE" privileges are needed to run this query. Warehouse will only suspend after it has finished running its queries.
ALTER WAREHOUSE audiencelab_main SUSPEND;

-- Statements to check what was executed in a warehouse (metadata, ACCOUNTADMIN needed):

SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(DATE_RANGE_START=>DATEADD('HOUR',-1,CURRENT_TIMESTAMP())));

SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(DATEADD('SEC',-500,CURRENT_DATE()),CURRENT_DATE()));

SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(CURRENT_DATE()));






-- Creating Databases, Schemas and Tables - Permanent, Transient and Temporary.

-- Databases
CREATE OR REPLACE DATABASE DEMO_DB; -- PERMANENT (fail-safe, retention period of 0-90 days. 0 disables it)

CREATE OR REPLACE TRANSIENT DATABASE DEMO_DB; -- TRANSIENT (no fail-safe, max retention of 1 day, can be disabled) -- all objects inside of database will be transient

CREATE OR REPLACE TEMPORARY DATABASE DEMO_DB; -- TEMPORARY (session-only, no fail-safe, no retention) -- all objects inside of database will be temporary

-- Schemas
CREATE OR REPLACE SCHEMA DEMO_DB.SOME_SCHEMA; -- PERMANENT 

CREATE OR REPLACE TRANSIENT SCHEMA DEMO_DB.SOME_SCHEMA; -- TRANSIENT 

CREATE OR REPLACE TEMPORARY SCHEMA DEMO_DB.SOME_SCHEMA; -- TEMPORARY 

-- Tables
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.SOME_TABLE; -- PERMANENT

CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.SOME_TABLE;  -- TRANSIENT 

CREATE OR REPLACE TEMPORARY TABLE DEMO_DB.PUBLIC.SOME_TABLE; -- TEMPORARY




-- Selecting data from tables. While in development, always use LIMIT clause, to reduce compute usage
SELECT * FROM SUPPLIER LIMIT 100;




-- MODULE 2 --


-- Caching and Query Profile Analyzing



-- In Query Profile:


-- 1) PROCESSING 

-- 2) LOCAL DISK I/O -- Local Storage Disk Layer -- Virtual Warehouse Machines' Disks. Compute spent to pull data from the select Warehouse's cache.

-- 3) REMOTE DISK I/O  -- Data Storage Layer. Compute spent to pull data from the deepest layer of snowflake.

-- 4) SYNCHRONIZATION 

-- 5) INITIALIZATION

-- 6) PERCENTAGE SCANNED FROM CACHE -- from 0 to 100% -- Cached Result Set Utilization (stored in Cloud Services Layer)





-- When a query is reused (Result set cache), no compute clusters (virtual warehoues) are used, as the result is fetched from the Cloud Services Layer.

-- Scanned Bytes = 0 --> Means that a result set, in the Cloud Services Layer, was used. No compute cost.


-- When a Warehouse is suspended, all Warehouse-internal caching is PURGED 
-- (but the Cloud Services Layer caching is not affected by this purge. CS layer result set cache, on the 
-- other hand, lasts for 24 hours).

-- Warehouse-internal retrieval of data by the usage of result caching is slower than the retrieval of data by usage of Cloud Services Layer result set caching
-- (the retrieval is slower because of processes of data encryption/decryption when transferring the data between the layers)





-- How layers and result set fetching (and caching) work:

-- 

-- RESULT CACHE (cloud services layer)
--     ^
-- LOCAL DISK CACHE (warehouse layer)
--     ^
-- REMOTE DISK (data storage layer, actual databases)

--


-- Manually disable Cloud Services Layer Caching Area (not recommended)
 ALTER SESSION 
 SET USE_CACHED_RESULT=FALSE;

-- Cloud Services Layer Result Set Cache lasts for 24 hours after a given query has been executed.



-- The default auto-suspend time is 600 seconds (10 minutes of inactivity suspends the warehouse).
-- Setting auto-suspend to 0 is not recommended (unless we have a workload of continuous usage) - Costs get huge, specially if we have larger warehouses.
ALTER WAREHOUSE
SET AUTO_SUSPEND=600;



-- During development activities, we should raise the auto-suspend period to at least 15 minutes (if warehouse is being heavily used), to maximize usage of warehouse caching.
ALTER WAREHOUSE
SET AUTO_SUSPEND=900;


-- If multiple user groups are working on the same set of tables (ex: pixel users and audience lab users are using the "TROVO" table), it may 
-- be a good idea to have both user groups use the same warehouse (maximize utilization of caching, to reduce costs).


-- If a given table is updated, its previous result set (of "SELECT *", for example) will not be used in future queries.


-- It is better to run multiple data altering statements in a single go (in a single transaction), instead of running them one by one, to reduce storage costs (failsafe)
-- and use result set caching to its fullest potential (if we run 5 update statements in a single day, updating a single row each time, the previous result set
-- will always be discarded, even if only a single record was altered/added/removed)



-- MODULE 3 --