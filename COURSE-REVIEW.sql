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

-- Shows the DATA DEFINITION LANGUAGE COMMAND (sql text) that was used to create this specific table
SELECT get_ddl('TABLE','<database.schema.table>');

-- Databases
CREATE OR REPLACE DATABASE DEMO_DB; -- PERMANENT (fail-safe, retention period of 0-90 days. 0 disables it)

CREATE OR REPLACE TRANSIENT DATABASE DEMO_DB; -- TRANSIENT (no fail-safe, max retention of 1 day, can be disabled) -- all objects inside of database will be transient

CREATE OR REPLACE TEMPORARY DATABASE DEMO_DB; -- TEMPORARY (session-only, no fail-safe, no retention) -- all objects inside of database will be temporary

-- Schemas
CREATE OR REPLACE SCHEMA DEMO_DB.SOME_SCHEMA; -- PERMANENT 

CREATE OR REPLACE TRANSIENT SCHEMA DEMO_DB.SOME_SCHEMA; -- TRANSIENT 

CREATE OR REPLACE TEMPORARY SCHEMA DEMO_DB.SOME_SCHEMA; -- TEMPORARY 

-- Tables
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.SOME_TABLE ( -- PERMANENT
    FIELD_A STRING NOT NULL, -- "not null" is the only constraint that is enforced, in Snowflake. All other constraints (even primary/foreign keys) are not enforced, and only kept as metadata
    FIELD_B NUMBER,
    FIELD_C DATE,
    FIELD_D VARIANT
); 

CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.SOME_TABLE ( -- TRANSIENT 
    FIELD_A STRING NOT NULL, -- "not null" is the only constraint that is enforced, in Snowflake. All other constraints (even primary/foreign keys) are not enforced, and only kept as metadata
    FIELD_B NUMBER,
    FIELD_C DATE,
    FIELD_D VARIANT
);  

CREATE OR REPLACE TEMPORARY TABLE DEMO_DB.PUBLIC.SOME_TABLE ( -- TEMPORARY
    FIELD_A STRING NOT NULL, -- "not null" is the only constraint that is enforced, in Snowflake. All other constraints (even primary/foreign keys) are not enforced, and only kept as metadata
    FIELD_B NUMBER,
    FIELD_C DATE,
    FIELD_D VARIANT
) 




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
ALTER WAREHOUSE <warehouse_name>
SET AUTO_SUSPEND=600;



-- During development activities, we should raise the auto-suspend period to at least 15 minutes (if warehouse is being heavily used), to maximize usage of warehouse caching.
ALTER WAREHOUSE <warehouse_name>
SET AUTO_SUSPEND=900;


-- If multiple user groups are working on the same set of tables (ex: pixel users and audience lab users are using the "TROVO" table), it may 
-- be a good idea to have both user groups use the same warehouse (maximize utilization of caching, to reduce costs).


-- If a given table is updated, its previous result set (of "SELECT *", for example) will not be used in future queries.


-- It is better to run multiple data altering statements in a single go (in a single transaction), instead of running them one by one, to reduce storage costs (failsafe)
-- and use result set caching to its fullest potential (if we run 5 update statements in a single day, updating a single row each time, the previous result set
-- will always be discarded, even if only a single record was altered/added/removed)





-- MODULE 3 --



-- Clustering Tips:


-- 1) Clustering is the process of grouping of records inside micro-partitions.

-- 2) Clustering is done automatically by Snowflake, using the order in which the data was inserted as basis.

-- 3) You can override the automatic clustering done by snowflake, by providing custom cluster keys.





-- Basic Syntax - Custom Clustering:



-- Create Table with Clustering:
CREATE TABLE EMPLOYEE (TYPE, NAME, COUNTRY, DATE) CLUSTER BY (DATE);


-- Alter already existing table, apply Clustering (will force the reordering of rows, in the table
-- Always be wary of the size of your table, as the compute cost for this reordering can be high):
ALTER TABLE EMPLOYEE CLUSTER BY (DATE);

-- This command is deprecated, as reclustering, nowadays, is done automatically by Snowflake itself.
ALTER TABLE TEST RECLUSTER;





-- In real life scenarios, your tables will have thousands of micro partitions.
-- These partitions will overlap with one another.
-- If there is more overlap, Snowflake has to scan through every one of these partitions (bad thing).
-- The degree of overlap, in Snowflake, is measured by the term "micro-partition depth"...
-- Our objective, to have effective clustering, is to have a high amount of "Constant Micro Partitions" (these are the partitions
-- that have already been clustered, whose micro-partition depth is equal to 1. 1 is the limit, we can't cluster 
-- more than that ).


-- How to check cluster status of a table:
-- "automatic_clustering" --> its value only will be "ON" if we apply custom clustering, by some column, in our table.
SHOW TABLES LIKE '<table_name>';

-- Shows if table is clustered (has cluster keys) or not ('000005 (XX000): Invalid clustering keys or table CUSTOMER_NOCLUSTER is not clustered')
SELECT SYSTEM$CLUSTERING_INFORMATION('<database.schema.table>');

-- Shows clustering information of given column in a table.
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_CLUSTERED','(C_MKTSEGMENT)');

SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_CLUSTERED');

-- It is also possible to cluster by PART of a column's value, such as a part of a date (ex: cluster by only the years, and not dates)
ALTER TABLE SAMPLE_DATABASE.PUBLIC.CUSTOMER_NOCLUSTER CLUSTER BY (C_MKTSEGMENT, substring(TO_DATE(date), 2)); -- we get "19" and "20", because of the "19XX" and "20XX" format.



-- Clustering Precautions (all requirements must be met, only then can we consider clustering):

-- 1) Clustering should not be applied to every table in a system.

-- 2) Table must be very large (multiple terabytes, large number of micro-partitions).

-- 3) The queries must be mostly SELECTIVE, and must frequently only read a small
-- percentage of rows (which means a small percentage of micro-partitions).

-- 4) Queries run on the table should frequently SORT the data (ORDER BY clauses).

-- 5) Most queries SELECT and SORT BY on the same few columns.

-- 6) The table must be queried (SELECT) frequently, but UPDATED infrequently.

-- Before clustering, Snowflake recomemends that we test a representative set of queries on the table, 
-- to have more info about the performance of the query, what can be done, etc.