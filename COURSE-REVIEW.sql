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

-- Snowflake Credit Cost varies by Region and Provider (AWS, Azure, GCP)

-- Force resume a warehouse. "OPERATE" privileges are needed to run this query.
ALTER WAREHOUSE audiencelab_main RESUME;

-- Force suspend a warehouse. "OPERATE" privileges are needed to run this query. Warehouse will only suspend after it has finished running its queries.
ALTER WAREHOUSE audiencelab_main SUSPEND;

-- Statements to check what was executed in a warehouse (metadata, ACCOUNTADMIN needed):
SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(DATE_RANGE_START=>DATEADD('HOUR',-1,CURRENT_TIMESTAMP())));

SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(DATEADD('SEC',-500,CURRENT_DATE()),CURRENT_DATE()));

SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(CURRENT_DATE()));






-- Great advantages of Snowflake:

-- 1) Warehouses are always available, and are decoupled from storage

-- 2) Excellent storage of metadata information (we can leverage that metadata to timetravel, build streams, dashboards, and more)





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
    FIELD_D VARIANT,
 --   CONSTRAINT PK_FIELD_A_ID PRIMARY KEY (FIELD_A) -- Does not exist in Snowflake (the only constraint that is enforced is NOT NULL).
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




-- Selecting data from tables. While in development, always use LIMIT clause, to reduce compute usage. Your result set must not exceed 10.000 rows, preferably.
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


-- Clustering -- 


-- Clustering Tips:


-- 1) Clustering is the process of grouping of records inside micro-partitions.

-- 2) Clustering is done automatically by Snowflake, using the order in which the data was inserted as basis.

-- 3) You can override the automatic clustering done by snowflake, by providing custom cluster keys.

-- 4) Clustering enforces a reordering of the rows in your table. This reordering will ALWAYS happen AFTER (gradually) each time data is loaded/updated in your table. 
-- This can be bad for costs in large tables, if you frequently update, as there will be a considerable amount of compute power cost each time there is a need for a reorder,
-- each time there is an update.





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

-- Shows clustering keys of your table (if they exist/are applied)
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_CLUSTERED');


-- {   "cluster_by_keys" : "LINEAR(C_MKTSEGMENT)",  
--  "total_partition_count" : 421,   
--  "total_constant_partition_count" : 0,   
--  "average_overlaps" : 420.0,  
--   "average_depth" : 421.0,  
--    "partition_depth_histogram" : 
--    {     "00000" : 0,     "00001" : 0,     "00002" : 0,     "00003" : 0,     "00004" : 0,     "00005" : 0,     "00006" : 0,     "00007" : 0,     "00008" : 0,     "00009" : 0,     "00010" : 0,     "00011" : 0,     "00012" : 0,     "00013" : 0,     "00014" : 0,     "00015" : 0,     "00016" : 0,     "00512" : 421   },   "clustering_errors" : [ ] }


-- Has two main uses: 1) Shows clustering information of given column in your table. 2) If a clustering key has not been applied on that column, Snowflake runs a simulation
-- of how well that column would perform, if used as a clustering key, without really applying it (but we should be careful, as this simulation is not always accurate).
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_CLUSTERED','(C_MKTSEGMENT)');

-- Example of a test that shows a bad clustering key idea:
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_NO_CLUSTER', '(C_MKTSEGMENT, C_CUSTKEY)');

-- {   "cluster_by_keys" : "LINEAR(C_MKTSEGMENT, C_CUSTKEY)", 
--   "notes" : "Clustering key columns contain high cardinality
--    key C_CUSTKEY which might result in expensive re-clustering. 
--    Please refer to 
-- https://docs.snowflake.net/manuals/user-guide/tables-clustering-keys.html 
-- for more information.",   
-- "total_partition_count" : 420,   
-- "total_constant_partition_count" : 0,  
--  "average_overlaps" : 419.0,  
--   "average_depth" : 420.0,   
--   "partition_depth_histogram" : {    
--      "00000" : 0,
--      ...    
--      "00512" : 420   },   "clustering_errors" : [ ] }


-- It is also possible to check/test Clusters applied on multiple columns at once.
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_NO_CLUSTER', '(C_MKTSEGMENT, C_CUSTKEY)');

-- It is also possible to cluster by PART of a column's value, such as a part of a date (ex: cluster by only the years, and not dates).
ALTER TABLE SAMPLE_DATABASE.PUBLIC.CUSTOMER_NOCLUSTER CLUSTER BY (C_MKTSEGMENT, substring(TO_DATE(date), 2)); -- we get "19" and "20", because of the "19XX" and "20XX" format.



-- Clustering Precautions (all requirements must be met, only then can we consider clustering):


-- 1) Clustering should not be applied to every table in a system.

-- 2) Table must be very large (multiple terabytes, large number of micro-partitions).

-- 3) The queries must be mostly SELECTIVE, and must frequently only read a small
-- percentage of rows (which means a small percentage of micro-partitions).

-- 4) Queries run on the table should frequently SORT the data (ORDER BY clauses).

-- 5) Most queries SELECT and SORT BY on the same few columns.

-- 6) The table must be queried (SELECT) frequently, but UPDATED infrequently.

-- 7) Clustering keys should be columns without high cardinality (avoid using IDs as clustering keys)

-- Before clustering, Snowflake recomemends that we test a representative set of queries on the table, 
-- to have more info about the performance of the query, what can be done, etc. Also use the "cluster test", seen above.




-- In what columns should we apply clustering?

-- 1) Columns frequently used with the "WHERE" clause in our queries.

-- 2) Columns frequently used with JOIN clause (as relational columns) and that do not have a high cardinality (ex: "Department_Id", and we have only 10 departments)

-- 3) The order specified in our clustering is also important, and is considered by Snowflake. 
-- Our columns, in the cluster key, should follow the order:
-- "Less cardinality" (unique values) -> "More cardinality" (unique values). 
-- It should be ordered like this because it is easier to group data by lesser amounts of distinct values.



-- How do we obtain the cardinality of a given column?


-- Run these commands:


-- Get total amount of records in a table (X)
SELECT COUNT(*) FROM <table_name>;

-- Get amount of distinct values for a column, in a table (Y).
SELECT COUNT(DISTINCT <col_name>) FROM <table_name>;

-- Divide Y by X:
SELECT Y/X; ---- example of output: 0.15555555555 (high cardinality, 15%).












-- MODULE 4 -- 


-- Improve performance without clustering --



-- The "auto-arrange" enforced, "under the hood", by the clustering feature of Snowflake
-- can be done by us, manually; We only need to order our rows by the would-be clustering keys, while our data is being loaded into a table/we are creating a table:
CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.CUSTOMER_ORDER_BY_EXAMPLE
AS 
SELECT * fROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER
ORDER BY C_MKTSEGMENT;

-- Checking Clustering Information (we will already have a high constant partition count, for that column. Data will already be arranged, without clustering)
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_ORDER_BY_EXAMPLE', '(C_MKTSEGMENT)');



-- Even if we DO apply custom clustering in a table, using Column X as a Cluster Key, we should, in the future, always use ORDER BY with that column
--  when loading our data into that table, as that will help save costs with compute 
-- (Snowflake won't have to rearrange the micropartitions, in the backend, automatically).



-- Still, this strategy is not the same as Clustering (won't be as beneficial).
-- Micro partitions in back-end will remain well grouped, yes, but there won't 
-- be a re-grouping of ALL micro-partitions based on every instance of recently loaded data. With this strategy,
-- the old micro-partitions won't be regrouped considering this new data. However, even so, the new data that is loaded into the 
-- table will have a better grouping, by itself, and that will improve your query times.






-- MODULE 5 -- 


-- Virtual Warehouses -- 




-- 1) They are essentially EC2 machines, running in the background.


-- 2) We can assign Warehouses to different users.


-- 3) If individual queries are too slow (complex individual queries), one strategy is to increase the size of our Warehouse.


-- 4) Auto-Scale Mode -- always recommended.

-- 4.1) To use it, we must specify a "minimum cluster count" (default: 1) and "maximum cluster count"...

-- 4.2) We must also be careful with the Auto-Scaling, as it can increase our costs considerably.


-- 5) Maximized Mode -- We enable this mode when we define the same values for "minimum warehouse count" and "maximum warehouse count". In this mode,
-- when the warehouse is started, Snowflake forces the initialization of all clusters, so that maximum resources are always available 
-- while the warehouse is running.

-- 5.1) This option should be used if you always have queries running in parallel, without varying traffic. 

-- 5.2) This mode can be viable, but you must have a lot of thought regarding the compute cost per hour and your bill.




-- Scaling Policy:




-- 1) "How many queries should be queued up, by Snowflake, to have an actual additional cluster started up?"

-- 2) "If no workload is present in it, when should a warehouse be suspended?"

-- 3) The scaling policy options, "Economy" and "Standard", are used for different use-cases/scenarios.

-- 3.1) Standard - The moment a query gets into a queue (a queue is formed), snowflake immediately creates copies of your cluster, to resolve this query.

-- 3.2) Economy -  Snowflake spins up additional clusters only if it estimates there's enough load to keep the clusters busy for at least 6 minutes.

-- 4) Snowflake checks, minute-by-minute, if the load in each warehouse's least loaded cluster
-- could be redistributed to other clusters, without spinning up the cluster again.
-- In each plan, the trigger to suspend a cluster is: 

-- 4.1) Standard - after 2-3 consecutive successful checks .

-- 4.2) Economy - after 5-6 consecutive successful checks. (time until clusters shuts down is longer, to keep cluster running and preserve credits)

-- 5) Essentially, "Economy" saves cost in the case of short spikes, but the user
-- may experience short to moderate queues, and delayed query execution.




-- MODULE 6 --




-- Performance Tuning --







-- The objective is to retrieve result sets more quickly.

-- In traditional databases (mySQL, PostgreSQL), we:


-- 1) Add indexes and primary keys.

-- 2) Create table partitions 

-- 3) Analyze query execution plan 

-- 4) Remove unecessary full table scans

-- 5) Verify optimal index usage

-- 6) Use hints to Tune Oracle SQL

-- 7) Self-order the table joins.



-- However, in Snowflake, a big part of optimization is done automatically.



-- What we must do is use Snowflake smartly.

-- Everything in Snowflake generates cost; me must not generate costs unecessarily.



-- In Snowflake, there is:


-- 1) No indexes.

-- 2) No primary/foreign key constraints.

-- 3) No constraints, besides "NOT NULL".

-- 4) No need for transaction management, as there is no Buffer Pool.

-- 5) No "out of memory" exceptions.




-- Ways to improve performance, in general:


-- 1) The less columns selected, the better (avoid the "*").

-- 2) If we are developing, we should use the same virtual warehouse, to save costs (usage of cached result sets).

-- 3) If we are running extremely large queries, a good idea is to break up the query in smaller logical units,
-- always trying to store the result of subqueries in transient/temporary tables, to save processing/compute costs.

-- 4) We should apply ORDER BY on our data, by the columns most used with WHERES and JOINS, before loading it 
-- into our tables. If we do so, even if we eventually apply clustering, in the future, the costs to reorder the 
-- table considering the clustering keys won't be as high, as the micropartitions
-- in the table will already be ordered, to some extent.

-- 5) If possible, when needed (too much queries at the same time, but not necessarily 
-- complex queries), always try to increase the max cluster count (multi-cluster warehouse)
-- instead of increasing the warehouse size (ex: from LARGE to XLARGE), as the costs will be much cheaper. This is 
-- also better than creating multiple warehouses (ex: multiple large warehouses) to do the same type of job/workload.





-- Snowflake treats transactions differently, but still satisfies the 4 A.C.I.D principles.




-- In Snowflake, update operations are a combination between DELETE and INSERT operations.






-- About update statements, ALWAYS BE CAREFUL. Two rules:



-- 1) Before running an update statement, check how many records are in your table, and how 
-- many of them would be impacted by the change. If 80% of the records would be impacted by the change,
-- you could/should consider recreating the entire table, with the correct data (because the total cost will probably 
-- be less than the UPDATE of all those records); alternatively, you could first DELETE all rows (truncate), to then 
-- INSERT the records with the correct/updated data (this also will be cheaper).

-- 2) When you are trying to DELETE or UPDATE rows in your tables, always try to use numeric columns as identifiers/
-- WHERE filters, because Snowflake's scanning mechanism is better suited/optimized for numbers (strings are a bad choice, 
-- for these operations).





-- To view queries blocked by other queries:
SHOW LOCKS;

-- Abort a transaction/statement (all statements, by themselves, are transactions).
SELECT SYSTEM$ABORT_TRANSACTION(<your_transaction_id>)

-- Cancel a query. mid-execution.
SELECT SYSTEM$CANCEL_QUERY(<your_query_id>)

-- Show open transactions with SESSION ID AND USER.
SHOW transactions IN account;

-- Kill all queries for the session.
SELECT SYSTEM$CANCEL_ALL_QUERIES(<your_session_id>);

--Aborts a session in our system.
SELECT SYSTEM$ABORT_SESSION(<your_session_id>)

-- How long can queries stay queried up (waiting for another query)?
SHOW PARAMETERS; -- "LOCK_TIMEOUT" -> is the amount of time allowed(1 hour and 10 minutes, basically).









-- MODULE 7 -- 



-- Snowsight (Graphical User Interface) and Dashboards



-- Some of your compute cost will always be associated to idle time. 
-- We should view that idle time in a dashboard, to optimize our Snowflake usage.



-- Metadata tables will be used to build the dashboard. The metadata tables used are:


-- 1) WAREHOUSE_METERING_HISTORY (view) 

-- 2) QUERY_HISTORY (view)

-- 3) WAREHOUSE_EVENTS_HISTORY (view) --> provides info about "when warehouse was suspended, when warehouse was started" (we can calculate idle time upon that).






-- This query can only be run with the "ACCOUNTADMIN" role. - We can use this query to view the amount of credits consumed
-- (and if we multiply by 2, 3, 4, we can get the amount of dollars spent, instead of credits).
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;

-- Returns a lot of metadata info about our queries. 
-- "Bytes_spilled_to_local_storage" and "bytes_spilled_to_remote_storage" --> if these values are high, the query is very performance-intensive.
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;

-- With this query, we can check when our warehouse was resumed, and when it was suspended. 
-- We can calculate the idle time, and check if our warehouses are being used appropriately.
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY
ORDER BY TIMESTAMP ASC;




-- For dashboard panel creation codes (in your worksheets), check Module 10 - Snowsight and Warehouse Dashboard.



-- In our worksheets, we can use "DYNAMIC FILTERS", accessed by the syntax ":filter_name". We can also create Custom filters, using the GUI.


-- To create and use a Dynamic Filter in the code of one of our worksheets, we must:

-- 1) Click in the small lines icon, in the upper-left corner of the worksheet editor page.

-- 2) Type the filter name and identifier ("example_filter" and ":example_filter", or "my_warehouse" and ":my_warehouse")

-- 3) Choose the warehouse that will run this filter.

-- 4) Click "Write query" 

-- 5) In this modal, you need to write a SELECT query that will retrieve the values that will be selectable, in the future,
-- with this filter, in the upper-left corner.

-- 5.1) Example: we want to have an option to select a warehouse, using our custom filter. For that, we need all the different warehouses of 
-- our system in a result set, so we write this: "SELECT DISTINCT warehouse_name FROM snowflake.account_usage.warehouse_metering_history;"

-- 5.2) With a list of possible values provided, by the running of this query, we have a checkbox that lets us choose between single 
-- values and "multiple values can be selected"

-- 6) Finally, with this dynamic filter created, we can apply it on any of our worksheets, if we use the syntax ":filter_name" (ex: "my_warehouse")

-- 6.1) Example of syntax usage:

SELECT SUM(COST) COST
FROM OVERVIEW
WHERE CRITERIA='SNOWFLAKE_WAREHOUSE_COST'
AND WAREHOUSE_NAME=:my_warehouse; -- custom filter (created by us) example.
  -- AND WAREHOUSE_NAME='COMPUTE_WH'; -- Example of possible value, inserted in the dynamic filter placeholder, by the Snowflake GUI.

-- 7) This feature of Dynamic Filters can also be used in our dashboards, to "filter by warehouse", or "filter by date", and other custom filters.





-- To create dashboards, we need to create them first, and then add our worksheets, as panels, to them.


-- Some quirks/tips:


-- 1) Once a worksheet is converted into a panel, it can't be reverted into a worksheet, so save your queries, beforehand, in other places.

-- 2) You should have a single query/statement per worksheet.

-- 3) The worksheets' names are always used as panel names, so name your worksheets accordingly.

-- 4) Each time we open our dashboard, the queries of the panels will be reexecuted (and will query the metadata tables).






-- How to read and analyze some of the dashboards' data:


-- 0) Remember, we can check utilization of warehouses by days/periods; for that, we must use the dynamic filters.

-- 1) "GB Written" --> if this number is greater than "GB written to result", this means our warehouse is being 
-- used mainly to load data into tables.

-- 2) "GB Written to result" --> if this number is greater than "GB Written", this means our warehouse is 
-- being used mainly to retrieve result sets with SELECT (select queries, Tableau, Snowflake web console).

-- 3) "GB Scanned" --> This number is usually accompanied by "GB Written to result" (SELECT queries). If this number 
-- is high and we have no "GB Written to result" (0 as a value), this means that some query was aborted whilst running (
-- bytes were scanned, but no result was retrieved; even if no result was retrieved, we'll have been charged by Snowflake all the same, for
-- the compute power).

-- 4) Warehouse classification:
-- 3.1) Very Active === Warehouses that are idle 25% of the time or less.
-- 3.2) Active === Warehouses that are between 25% and 75% of the time idle.
-- 3.3) Dormant === Warehouses that are 75% or more of the time idle.

-- 5) You should be wary of each type of data use, and its values. If the values are too high, it may indicate a serious problem with your queries:

    -- 5.1) "GB Written" too high - This means that a lot of data is being inserted/updated on Snowflake. You should think about costs with storage,
    -- both active bytes, time travel bytes and failsafe bytes, which can generate a very high combined cost. You must ensure that this is really business 
    -- critical data, and not junk data.

    -- 5.2) "GB Written to result" too high - This means that your queries are probably written incorrectly, and are retrieving/writing data to the Snowflake 
    -- Web Console (which is a very bad thing, because the console/GUI can only show up to 1.5 million records per query; the rest of the records is not 
    -- shown/outputted). Consider running aggregations (like SUM() or COUNT()) to produce your results, as that won't envolve the write of unecessary 
    -- data in the Snowflake Web Console. Your result set must not exceed 10.000 rows, preferably.

    -- 5.3) "GB Scanned" too high - This may indicate that you wrote inefficient queries, that they are pulling almost all of the records in the table,
    -- or scanning almost all partitions.

-- 6) "GB_SPILLED_TO_LOCAL_STORAGE" too high, in a query - This means that your query is too demanding, works with too much data, and the current 
-- warehouse is not able to support/execute your queries satisfactorily. You should use a larger warehouse, or process your data in smaller batches.

https://github.com/dbt-labs/docs.getdbt.com/discussions/1550

"One of the biggest killers of performance in Snowflake is queries spilling to either local or remote storage.
This happens when your query processes more data than your virtual warehouse can hold in memory,
and is directly related to the size of your warehouse."


-- Possible Solutions:

-- A) Throw resources at it, and hope it goes away. This will cost you money, but can be a
-- quick fix if you need a solution ASAP. The amount of memory that Snowflake has available
-- for a given query is governed by warehouse size, so if you up the warehouse, you up your
-- memory.


-- B) Process your data in smaller chunks. By limiting the amount of data that a query
-- processes you can potentially prevent spilling anything to local/remote storage.


-- C) Watch out for big CTEs. (WITH clauses in our SELECTs) If you're processing a ton of data
-- in multiple CTEs in the same query, there's a good chance you'll hit this problem. 
-- Since CTEs process their results in memory, it hogs that resource for the query. 
-- Try converting your largest CTEs into views and see if that solves the problem.



-- 7) "GB SPILLED TO REMOTE STORAGE" too high, in a query - Same thing as Point 6, your query is probably too demanding. The assigned compute 
-- may not be enough.




-- MODULE 8 --




-- Query Acceleration Service (QAS)






-- How to enable (SQL code):

CREATE WAREHOUSE <warehouse_name>
ENABLE_QUERY_ACCELERATION = TRUE
QUERY_ACCELERATION_MAX_SCALE_FACTOR = <num>;

ALTER WAREHOUSE <warehouse_name>
SET ENABLE_QUERY_ACCELERATION = TRUE
QUERY_ACCELERATION_MAX_SCALE_FACTOR = <num>; -- multiplier


"It can accelerate parts of the query workload in a warehouse. When it is enabled for a warehouse,
it can improve overall warehouse performance by reducing the impact of outlier queries, which are queries 
that use more resources than the typical query. The query acceleration service does this by offloading 
portions of the query processing work to shared compute resources that are provided by the service.
It can handle workloads more efficiently by performing more work in parallel and reducing the Walltime
spent in scanning and filtering".




-- The usage of QAS can be cheaper than increasing the size of your warehouse, but it can still be expensive (or even more expensive,
-- if used incorrectly).



-- This service indirectly improves the speed of our read operations.


-- It is best used to improve execution times of queries that spend a lot of time/processing with "REMOTE DISK I/O" (extracting data out of 
-- the deepest layer of Snowflake, in the AWS S3 Blob storage).



-- One of QAS' advantages is its flexibility, which is greater than the increase/decrease of a warehouse's size.


-- Its flexibility is provided by the "Scale Factor", 
-- a COST CONTROL mechanism that allows you to set an upper bound 
-- on the amount of compute resources a warehouse can LEASE 
-- (Snowflake borrows us machines, for the sole 
-- purpose of increasing query speed) for query 
-- acceleration. This value is used as a MULTIPLIER 
-- based on WAREHOUSE SIZE and COST.



-- Example: scale factor of 5 for a MEDIUM-sized warehouse --> this means that this warehouse can borrow 
-- resources up to 5 times its size. (and 5 times the cost, totalling up to 20 credits per hour, 4 x 5).



-- This means that QAS is essentially a multiplier, based on the currently selected warehouse size.

-- It should be used when query takes more time with "Remote Disk I/O", with the extraction of data from the storage layer (table scan).


-- Before we utilize/apply the service, we should check if our queries are ELIGIBLE for its use, with this code (ACCOUNTADMIN role needed):
-- Also, the query must have been executed before, so we can get its query_id string.
SELECT PARSE_JSON(system$estimate_query_acceleration('<query_id>'));


-- The outputted JSON's format:

-- {"originalQueryTime": 252
-- "eligible": true,
-- "upperLimitScaleFactor": 1
-- }

-- Check more queries, see if they are eligible for Query Acceleration Service.
-- If a lot of queries are eligible, and if the calculated costs are sensible, we can consider it.
SELECT * FROM snowflake.account_usage.query_acceleration_eligible
ORDER BY eligible_query_acceleration_time DESC;





-- However, the Query Acceleration Service brings with it two important caveats:

-- 1) Only fetching (SELECT) and filtering operations are affected by the acceleration (UPDATEs and DELETEs don't get impacted).
-- The best-use case for it is queries that spend 75%-80% of the time in full table scans.


-- 2) When using QAS, queries will no longer be able to benefit from Warehouse Caching (because the machines used by/with QAS 
-- will be borrowed machines, different from the machines of your warehouse's cluster)








-- MODULE 9 -- 





-- Search Optimization Service (SOS) --




-- How to enable (SQL code):


ALTER TABLE <table_name> ADD SEARCH OPTIMIZATION ON <col_name>;





"Essentially, this service's objective is to find needles (few records) in haystacks (huge tables)".


"The search optimization service can significantly improve the 
performance of certain types of lookup and analytical queries that 
use an extensive set of predicates for filtering."

"Selective point lookup queries on tables.
A point lookup query returns only one 
or  a small number of distinct rows."

"Once you identiy the queries that can benefit from the 
search optimization service, you can configure 
search optimization for the columns and tables 
used in those queries."


-- Used in scenarios in which:

-- 1) You have a huge table (50gb, 150gb, 250gb+ ), and you frequently return only a few records from it. (50 records out of 20 million, for example)

-- 1.1) "Few records" --> try to retrieve at most 1k records; the SOS is not recommended for the retrieval of result sets greater than that count.

-- 2) You are frequently running intensive analytical queries, with a lot of WHERE conditions in 
-- a single query (because the more WHERE conditions you have, the less records you retrieve).

-- 3) Too slow individual queries.

-- 4) You don't want to spend costs in a bigger warehouse.

-- 5) Your queries are not eligible for QAS (Query Acceleration Service)

-- 6) Clustering is not viable, and loading data while ordering (ORDER BY) it is not good enough.


-- Example code:


-- Create table with 6 billion rows.
CREATE TABLE DEMO_DB.PUBLIC.LINEITEM_SOS
AS
SELECT * FROM
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM;

-- Clone original table's structure and data (zero-copy-clone)
CREATE TABLE DEMO_DB.PUBLIC.LINEITEM_NO_SOS CLONE DEMO_DB.PUBLIC.LINEITEM_SOS;

-- Add search optimization on certain columns - this creates/uses extra storage, so be careful (185gb table gets 30gb extra storage, for these 2 columns with SOS).
ALTER TABLE DEMO_DB.PUBLIC.LINEITEM_SOS ADD SEARCH OPTIMIZATION ON EQUALITY(L_COMMENT);
ALTER TABLE DEMO_DB.PUBLIC.LINEITEM_SOS ADD SEARCH OPTIMIZATION ON EQUALITY(L_ORDERKEY);

-- Column "search_optimization" (ON/OFF). Also "search_optimization_bytes", which shows how much storage bytes (additional storage) is being spent with SOS.
SHOW TABLES;

-- Shows the difference between search optimization enabled and disabled:
SELECT * FROM DEMO_DB.PUBLIC.LINEITEM_SOS WHERE L_ORDERKEY = '4509487233';  -- Takes 6 seconds, roughly. - 3 partitions scanned.
select * from DEMO_DB.PUBLIC.LINEITEM_NO_SOS where L_orderkey = '4509487233'; -- Takes 43 seconds, roughly. - 9 thousand partitions scanned



-- Essentially, Snowflake creates additional "lookup tables" (additional storage costs) to help improve your query speed.
-- These additional "lookup tables" function similarly to regular index tables in conventional database systems (but not identically, as these lookup 
-- tables do not have UNIQUEness and NOT NULL constraints, as index tables do).

-- These lookup tables are materialized views, created using the selected columns as a basis, and have a storage cost associated to them.








-- RECAP of query optimization options, so far: --

-- 1) Clustering - The grouping of micropartitions according to the most used filters

-- 2) Query Acceleration Service (QAS) - Used to reduce query times on huge tables, huge amounts of data retrieved. "Borrows" additional machines to read data faster.

-- 3) Search Optimization Service (SOS) - The creation of an index-like additional table (materialized view), 
-- which helps lookup few rows (many WHERE filters) in huge tables.












-- MODULE 10 -- 



-- Load data - Intro 




-- To work with Snowflake in our terminal, we need:

-- 1) Snow CLI 

-- 2) AWS CLI 




-- To connect to our snowflake account/app, we must run, in the terminal:
snowsql -a <account-identifier> -u <username_in_the_account>  -- "account-identifier" is something like <string>.us-east-2.aws










-- MODULE 11 -- 


-- Before loading data into our tables, we must create the auxiliary objects that will be used with the COPY command.
-- As a best practice, we should create a dedicated database, where all these objects will be stored, a central place.
-- This will greatly help us in the future, when we need to referenec them in our COPY commands:


-- Create a dedicated database for our Snowflake Objects (we don't need the failsafe feature, so we create it as transient)
CREATE TRANSIENT DATABASE CONTROL_DB;

-- Create Schemas for each of the Snowflake Object types
CREATE SCHEMA CONTROL_DB.INTERNAL_STAGES;
CREATE SCHEMA CONTROL_DB.EXTERNAL_STAGES;
CREATE SCHEMA CONTROL_DB.INTEGRATION_OBJECTS; -- Storage integration objects
CREATE SCHEMA CONTROL_DB.FILE_FORMATS;
CREATE SCHEMA CONTROL_DB.MASKING_POLICIES;
CREATE SCHEMA CONTROL_DB.PIPES;





-- Load data - First Object Type - Stages 



-- Stages are Snowflake objects that represent Blob Storage Areas (like S3),
-- places where you load/upload all your raw files, before loading them into Snowflake tables.


-- Stages contain properties like "location", which is the place where your files will be coming from.


-- There are 2 types of Stages:



-- 1) Internal Stages (staging areas managed by Snowflake itself)

    -- 1.A) Table Staging Areas - Symbols are "@%"

    -- 2.B) Named Staging Areas - Symbol is "@"

    -- 3.C) User Staging Areas (rarely used) - Symbols are "@~"

-- 2) External Stages (staging areas managed by third parties, such as S3, GCP, Azure.) - Symbol is "@"







-- Unlike Internal Stages, External Stages must be prepared before being used. This preparation involves the creation of a Integration Object,
-- which is responsible for making the connection between Snowflake and S3, GCP, Azure, secure.

-- One best practice is the usage of file format objects, which avoid repetition of code in your COPY commands.

-- The most used Stages (from most used to least used) are External Stages, Named Stages and Table Stages.




-- A) Table Stages (least used):


-- Each table has a snowflake stage allocated 
-- to it, by default, for storing files. This stage 
-- is a convenient option if your files need to be 
-- accessible to multiple  users and only need 
-- to be copied into a single table.


-- This type of Stage is automatically created and assigned to each corresponding table.


-- They should be used when:

-- 1) We have multiple users in our account.

-- 2) We have multiple files in this stage.

-- 3) All the data in the stage will be copied only to this single table, no COPYs to other tables.




-- Some unique traits of Table Stages:

-- 1) Unlike Named and External Stages, they cannot be dropped, as they are part of the table objects.

-- 2) Unlike Named and External Stages, we cannot use File Format objects with them; if you want a 
-- specific FILE_FORMAT, you must write it inline, like this:

COPY INTO xxx 
FROM @yyy
FILE_FORMAT=(
    SKIP_HEADER=1,
    TYPE=CSV
);

-- 3) They do not support data transformations while loading data into your tables.





-- B) Named Stages 


-- Named stages are database objects that provide 
-- the greatest degree of flexibility for data loading.
-- Because they are database objects, the security/access 
-- rules that apply to all objects also are applied 
-- to this type of object.





-- The great advantages of this Stage type are:


-- 1) They can be used to load data into any of your tables.

-- 2) As they behave like regular snowflake objects, we can grant/revoke access, to them, to our various account roles (better access control).

-- 3) A common best practice is the creation of folders insided of this stage, so we can atribute each folder to a table, inside our Snowflake system.


-- Basic Stage Creation Syntax:



-- Create Internal, Named Stage
CREATE OR REPLACE STAGE CONTROL_DB.INTERNAL_STAGES.MY_INT_STAGE;

-- Create External Stage - insecure (no Integration Object)
CREATE OR REPLACE STAGE CONTROL_DB.EXTERNAL_STAGES.MY_EXT_STAGE 
    url='s3://snowflake867/test/';

-- Create External Stage - secure (with Integration and File Format Objects)
CREATE OR REPLACE STAGE CONTROL_DB.EXTERNAL_STAGES.MY_EXT_STAGE 
    url='s3://snowflake867/test/'
    STORAGE_INTEGRATION=<integration_name> -- Integration Object needed
    FILE_FORMAT=(
        FORMAT_NAME=<format_name> -- File Format Object needed
    );

-- Alter Stage Object
ALTER STAGE CONTROL_DB.INTERNAL_STAGES.MY_INT_STAGE
    SET FILE_FORMAT=(
        TRIM_SPACE=TRUE
    )
    COPY_OPTIONS=(PURGE=TRUE); -- with "PURGE=TRUE", the files in our stage will be DELETED after a successful COPY operation.

-- Drop Stages
DROP STAGE CONTROL_DB.INTERNAL_STAGES.MY_INT_STAGE;
DROP STAGE CONTROL_DB.INTERNAL_STAGES.MY_EXT_STAGE;

-- Describe Stages' properties (location, database, schema, name, etc)
DESC STAGE CONTROL_DB.INTERNAL_STAGE.MY_INT_STAGE; -- "location" will be empty (as we are inside of snowflake)
DESC STAGE CONTROL_DB.INTERNAL_STAGE.MY_EXT_STAGE; -- "location" will be your bucket's url.

-- List files inside of stage
LIST @CONTROL_DB.INTERNAL_STAGE.MY_INT_STAGE;
LIST @CONTROL_DB.INTERNAL_STAGE.MY_EXT_STAGE;

-- Show all Stages 
SHOW STAGES;




-- Load data - Second Object Type - File Formats




-- The greatest advantage of the File Format objects is 
-- that it does not matter how many COPY commands you have,
-- if you change the File Format that is registered to all of them,
-- the changes's effects will be applied to all of the commands as well.




-- Basic File Format Creation Syntax:




-- Create CSV File Format
 CREATE OR REPLACE FILE FORMAT CONTROL_DB.FILE_FORMATS.CSV_FORMAT
    TYPE=CSV,
    FIELD_DELIMITER=',',
    SKIP_HEADER=1,
    NULL_IF=('NULL', 'null')
    EMPTY_FIELD_AS_NULL=true
    COMPRESSION=gzip; -- for files in ".csv.gzip" format


-- Alter CSV File Format - Example
ALTER FILE FORMAT CONTROL_DB.FILE_FORMATS.MY_CSV_FORMAT
    SET TYPE='JSON',
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;

-- Describe File Format Object's properties - many of them are also present in stage object, but we should always try to define properties'
-- values in the File Format objects, and not stages (best practice - you should try not to write file_format argument inline, in copy command)
DESC FILE FORMAT CONTROL_DB.FILE_FORMATS.MY_CSV_FORMAT;

-- Show all File Formats 
SHOW FILE FORMATS;






-- Load data - Third Object Type - Integration Object


-- These are always needed for external stages, to have a secure connection between AWS and S3.



-- Basic Integration Object Creation Syntax:



-- Create Integration Object
CREATE OR REPLACE STORAGE INTEGRATION <integration_name>
    TYPE=EXTERNAL_STAGE
    STORAGE_PROVIDER=S3
    ENABLED=TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::*******************:role/snowflake' -- a "snowflake" dedicated IAM user is needed, in AWS, to utilize this value
    STORAGE_ALLOWED_LOCATIONS=('<bucket-url>'); -- create bucket beforehand

-- Alter Integration Object 
ALTER STORAGE INTEGRATION S3_<integration_name>
SET STORAGE_ALLOWED_LOCATIONS=(
    's3://bucket-url/folder-1/',
    's3://bucket-url/folder-2/',
    's3://bucket-url/folder-3/'
);

-- Describe Integration Object (mandatory, as we need the STORAGE_AWS_EXTERNAL_ID and
-- STORAGE_AWS_ROLE_ARN; we'll use this ID and this role in the AWS config, in IAM users/buckets, in "Trusted Relationships")
DESC STORAGE INTEGRATION <integration_name>;

-- Integration Object fields:
property	                    property_type	property_value	            property_default
ENABLED	                        Boolean	            true	                    false
STORAGE_PROVIDER	            String	            S3	
STORAGE_ALLOWED_LOCATIONS	    List	s3://new-snowflake-course-bucket/CSV/	[]
STORAGE_BLOCKED_LOCATIONS	    List		        []
STORAGE_AWS_IAM_USER_ARN	    String	arn:aws:iam::543875725500:user/heeb0000-s	
STORAGE_AWS_ROLE_ARN	        String	arn:aws:iam::269021562924:role/new-snowflake-access	
STORAGE_AWS_EXTERNAL_ID	        String	   UU18264_SFCRole=2_TBE7RjHPfSqmCjne1y5exkh5IDQ=	
COMMENT	                        String		

-- In AWS, create IAM user and Role, policy "s3FullAccess", and edit the permmissions' JSON:
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::269021562924:role/new-snowflake-access"
			},
			"Action": "sts:AssumeRole",
			"Condition": {
				"StringEquals": {
					"sts:ExternalId": "UU18264_SFCRole=2_TBE7RjHPfSqmCjne1y5exkh5IDQ="
				}
			}
		}
	]
}

-- Finally, after setting up AWS and Snowflake with this 
-- Storage Integration Object, create a secure Stage Object and validate/check if connection between two systems is valid

-- Create Stage Object
CREATE OR REPLACE STAGE CONTROL_DB.EXTERNAL_STAGES.MY_EXT_STAGE 
    url='s3://snowflake867/test/'
    STORAGE_INTEGRATION=<integration_name> -- our Integration Object
    FILE_FORMAT=(
        FORMAT_NAME=<format_name>
    );

-- Check Connection Between Systems
LIST @CONTROL_DB.EXTERNAL_STAGES.MY_EXT_STAGE; -- will show the files in our bucket.








-- Uploading files manually (via GUI and via Snow CLI):



-- Using the Snowflake Web Console GUI, we can upload files, from our local machines,
-- directly into our Snowflake tables; this practice is only recommended if you have up to 10k records.
-- If we have more than that, the Snow CLI and its commands must be used.



-- Example of Snow CLI usage, to upload files:


-- Create Table (and, consequently, Table Stage)
CREATE TABLE DEMO_DB.PUBLIC.EMP_BASIC (
    FIELD_1 STRING,
    FIELD_2 NUMBER,
    FIELD_3 DATE
);

-- Upload file (.csv) from local storage to Snowflake Internal Stage (Table Stage), blob storage, using Snow CLI
 PUT FILE:///root/Snowflake/Data/Employee/employees0*.csv -- local filesystem path
 @DEMO_DB.PUBLIC.%EMP_BASIC; -- stage

-- List files, now present in Table Stage's blob storage area
LIST @DEMO_DB.PUBLIC.%EMP_BASIC; -- in worksheets
ls @DEMO_DB.PUBLIC.%EMP_BASIC; -- in Snow CLI

-- Remove/delete files, present in Table Stage's blob storage area (to save storage costs), after their data has been copied to your tables
REMOVE @DEMO_DB.PUBLIC.%EMP_BASIC; -- in worksheets 
rm @DEMO_DB.PUBLIC.%EMP_BASIC; -- in Snow CLI

-- Select rows in EMP_BASIC Table (the result set will be empty, just like the table, as the files will still only exist in the Table Stage's blob storage)
SELECT * FROM DEMO_DB.PUBLIC.EMP_BASIC LIMIT 100;





-- Example of Snow CLI usage, to download files (from Table and Named stages):


-- Download file (.csv) from Table Stage to local storage
GET @DEMO_DB.PUBLIC.%EMP_BASIC
file:///path/to/your/local/file/storage/that/will/receive/the/file;




-- Before summarizing the Copy command and its features,
-- it is good to review the use-case in which we do not 
-- copy the data into Snowflake tables, but we use Snowflake
-- to query the data from files, stored in External Stages (S3).
-- With this approach, a lot of Snowflake's features are wasted.
-- Some of these features are caching, the storing of metadata and 
-- micropartitions. This approach should be used only if our data 
-- in S3 is rarely queried (can be advantageous in cases where 
-- you want to avoid the data storage costs in both Snowflake and S3).
-- Also, if this approach is used, the "Query Profile" option will have 
-- very few details about the query.




-- Example of querying data from an External Stage directly, without copying into a table
SELECT 
T.$1 AS first_name,
T.$2 AS last_name,
T.$3 AS email
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_EXTERNAL_STAGE AS T;

-- Querying while filtering
SELECT
T.$1 AS first_name,
T.$2 AS last_name,
T.$3 AS email 
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_EXTERNAL_STAGE AS T
WHERE T.$1 IN ('Di', 'Carson', 'Dana');

-- Querying while joining
SELECT 
T.$1 AS first_name,
T.$2 AS last_name,
T.$3 AS email
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_EXTERNAL_STAGE AS T
INNER JOIN @CONTROL_DB.EXTERNAL_STAGES.S3_EXTERNAL_STAGE AS D
ON T.$1=D.$1;

-- You can also create views - when new files are added to your bucket, the view will "refresh" automatically (because it only saves the logic of the query, not the results)
CREATE OR REPLACE VIEW DEMO_DB.PUBLIC.QUERY_FROM_S3 
AS 
SELECT
T.$1 AS first_name,
T.$2 AS last_name,
T.$3 AS email 
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_EXTERNAL_STAGE AS T;

-- You can also create a table from the bucket's files - However, when new files are eventually added to your bucket, this table won't be refreshed.
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.QUERY_FROM_S3_TABLE
AS 
SELECT
T.$1 AS first_name,
T.$2 AS last_name,
T.$3 AS email
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_EXTERNAL_STAGE AS T;












-- The Copy Command


-- One quirk of the copy command, very important to know, is that 
-- Snowflake stores the md5 hash values of each of the files loaded into your tables.
-- If you are trying to load the same file repeatedly, Snowflake will stop you: it will 
-- compare the md5 value of the to-be-loaded file and the md5 value of the file you already
-- loaded, find out they are equal, and then will stop the load proccess. This is done to 
-- avoid the creation of duplicate rows in our tables. To bypass this behavior, if needed, 
-- we must set the option "FORCE=TRUE" in our COPY commands.



-- Copying into Snowflake tables from S3 External Stage, examples:



-- Copying from AWS External Stage, most basic format:
COPY INTO <table_name> -- create table beforehand (with appropriate columns and data types for each column)
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_STAGE -- create stage beforehand
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT -- create file format beforehand
);


-- Copying from AWS External Stage, forcing the repeated copy procedure of a file:
COPY INTO <table_name>
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_STAGE
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT
)
FORCE=TRUE;


-- Copying from AWS External Stage, CSV file, only some of the columns of files:
COPY INTO <table_name>
FROM (
    SELECT
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6
        FROM @CONTROL_DB.EXTERNAL_STAGES.S3_STAGE  AS t ) 
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT
);


-- Copying from AWS External Stage, CSV file, metadata (filename and file_row_number, to assist in migrations;
-- with these values, we know exactly from which .csv file our rows were extracted) and some of the file's columns:
COPY INTO <table_name>
FROM (
    SELECT
        METADATA$FILENAME AS FILE_NAME,
        METADATA$FILE_ROW_NUMBER,
        t.$1,
        t.$2,
        t.$3
        FROM @CONTROL_DB.EXTERNAL_STAGES.S3_STAGE  AS t ) 
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT
);


-- The same as the code seen above, but better - FILE_NAME column's values are cleaner. 
-- ("@employees03.csv.gz" format, instead of "@emp_basic_local/employees03.csv.gz")
COPY INTO <table_name>
FROM (
SELECT 
SPLIT_PART(METADATA$FILENAME, '/', 2) AS FILE_NAME,
METADATA$FILE_ROW_NUMBER,
T.$1,
T.$2,
T.$3
FROM @CONTROL_DB.EXTERNAL_STAGES.S3_STAGE  AS t 
)
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT
);


-- After data was copied from AWS into Snowflake table, we can validate/check if all rows were loaded, with this simple SQL statement:
SELECT 
DISTINCT FILE_NAME AS FILE_NAME,
COUNT (*) AS AMOUNT_OF_ROWS
FROM <table_name> -- staging table, filled by the above statement
GROUP BY FILE_NAME;







-- Copying from Snowflake Internal Stages (Table and Named Stages) into Snowflake Tables, examples:





-- Copy data into Snowflake table, from Internal (Table) staging area
COPY INTO DEMO_DB.PUBLIC.EMP_BASIC
FROM @DEMO_DB.PUBLIC.%EMP_BASIC
FILE_FORMAT= (
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT;
);

-- Situational command, lets us view how our rows are being formatted, in the files present in our table staging area.
SELECT
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
    FROM @DEMO_DB.PUBLIC.%EMP_BASIC
    (
        FILE_FORMAT => CONTROL_DB.FILE_FORMATS.CSV_FORMAT; -- to view the data correctly, we need to write the file format in, in this manner.
    )
    LIMIT 100;

-- After loading the data, with this command, we can compare the loaded data with the data in the table staging area;
-- if no rows are returned, the data was copied perfectly, and the two sets are identical.
SELECT
$1,
$2,
$3,
$4,
$5,
$6 
FROM @DEMO_DB.PUBLIC.%EMP_BASIC
(FILE_FORMAT => CONTROL_DB.FILE_FORMATS.CSV_FORMAT)
MINUS -- compares the data in the files in the table staging area to the already loaded data, loaded from the same files.
SELECT * FROM DEMO_DB.PUBLIC.EMP_BASIC;







-- Copy data into Snowflake table, from Internal (Named) staging area
COPY INTO DEMO_DB.PUBLIC.EMP_BASIC
FROM @DEMO_DB.PUBLIC.EMP_BASIC
FILE_FORMAT= (
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT;
);

-- Feed files into Named Stage, from file system, for future use (copy into tables)
put 'file:///path/to/your/local/file/storage/that/will/upload/the/files/*'
@CONTROL_DB.INTERNAL_STAGES.MY_INT_STAGE/EMP_BASIC;

-- Describe Stage Object:
DESC STAGE DEMO_DB.INTERNAL_STAGES.MY_INT_STAGE










-- The inverse way ("unload" of data), copying/transferring data from Snowflake tables to Stages (Internal, External), examples:



-- Copy data from Snowflake table (tabular data) into Table Stage (csv files, parquet data, json, etc)...
COPY INTO @DEMO_DB.PUBLIC.%EMP_BASIC
FROM DEMO_DB.PUBLIC.EMP_BASIC
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT
);

-- Copy only some columns of your table into Table Stage
COPY INTO @DEMO_DB.PUBLIC.%EMP_BASIC
FROM (
    SELECT
    FIRST_NAME,
    LAST_NAME,
    EMAIL
    FROM  DEMO_DB.PUBLIC.EMP_BASIC
)
FILE_FORMAT=(
    FORMAT_NAME=CONTROL_DB.FILE_FORMATS.CSV_FORMAT
)
-- OVERWRITE=TRUE; -- used if you want to replace a file that is already living in the stage area.


-- Downloads the files to your local system, in csv/json/parquet format; these are the files now residing 
-- in the  Table Staging Area. This command can only be used inside Snow CLI (does not work in worksheets)
GET @DEMO_DB.PUBLIC.%EMP_BASIC
file:///path/to/your/local/file/storage/that/will/receive/the/file;








-- MODULE 12 -- 


-- Basic Error Handling During COPY command process --


-- In real-life scenarios, it is extremely common 
-- to receive errors during the execution of Copy commands.
-- The errored-out records must not be ignored, and should 
-- preferably be stored in an additional table, so they 
-- can be debugged in the future.

-- To achieve this goal, we must use the "ON_ERROR='CONTINUE'"
-- option, in our COPY command.




-- Error handling example: 

-- Create Staging Table
CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.EMP_BASIC (
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    STREETADDRESS STRING,
    CITY STRING,
    START_DATE DATE
);

-- Continue copying, even with errors ("PARTIALLY LOADED")
COPY INTO DEMO_DB.PUBLIC.EMP_BASIC
FROM (
    SELECT 
    T.$1,
    T.$2,
    T.$3,
    T.$4,
    T.$5,
    T.$6
    FROM @CONTROL_DB.EXTERNAL_STAGES.MY_EXT_STAGE AS T
)
ON_ERROR='CONTINUE';

-- Use "VALIDATE()" function, to show which records errored-out during the copy
SELECT * FROM TABLE(VALIDATE(DEMO_DB.PUBLIC.EMP_BASIC, JOB_ID => <your_query_id>));

-- Create a table, "REJECTED_RECORDS" with the format 'error_message - rejected record'
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.REJECTED_RECORDS 
AS 
SELECT * FROM TABLE(VALIDATE(DEMO_DB.PUBLIC.EMP_BASIC, JOB_ID => <your_query_id>));

-- Create another table, without the error messages, and only the rejected record's values in the columns.
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.FORMATTED_REJECTED_RECORDS AS
SELECT 
SPLIT_PART(rejected_record, ',', 1 ) as first_name,
SPLIT_PART(rejected_record, ',', 2 ) as last_name,
SPLIT_PART(rejected_record, ',', 3 ) as email,
SPLIT_PART(rejected_record, ',', 4 ) as streetaddress,
SPLIT_PART(rejected_record, ',', 5 ) as city,
SPLIT_PART(rejected_record, ',', 6 ) as start_date
FROM DEMO_DB.PUBLIC.REJECTED_RECORDS;