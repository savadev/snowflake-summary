-- If the company ever needs to set up everything in a new Snowflake Account again, the queries shown in the files of this folder 
-- should provide a substantial aid in the process.


-- The purpose of this specific file, on the other hand, is to help with the understanding of the Snowflake-side of the 
-- Audiencelab platform, how it communicates with our backend, what processes are currently 
-- running on our Snowflake Account, and how everything is glued together.


-- Before going through the steps that should be taken to set up everything anew, in a new account, it is best 
-- to describe the current database configuration. Basically, the most important elements are:


-- STATIC ELEMENTS (elements that don't change over time; in other words, elements which don't have tasks, streams or pipes attached to them; also, objects that don't need commands to suspend/start):


-- A) The Warehouses currently being used

-- B) The Databases, Schemas and Tables currently being used with the platform's queries 

-- C) The Queries which are currently reaching the Account's warehouses

-- D) Users, Roles and Privileges

-- E) Other objects (Streams, Pipes, Procedures, Stages, Storage Integrations)





-- Let's go through the Static Elements, one by one.




-- A) Warehouses


-- Our current account uses nine different warehouses, of varying sizes, according to the
-- processing power needed to execute the queries that arrive on each one.

-- The Warehouse creation syntax, as seen on document "1-WAREHOUSES.sql":

CREATE OR REPLACE WAREHOUSE ANALYST_WH with
warehouse_size='SMALL'
auto_suspend = 60
auto_resume = true
initially_suspended=true;

-- This Warehouse is to be used when we are, as developers, analyzing the content 
-- of any of our tables, the state of our objects, etc. We should use this warehouse 
-- for two reasons:

-- 1) To avoid occupying our other production warehouses (AUDIENCE_WH, PIXEL_WH, ENRICHMENT_WH) 
--    with unecessary queries, something that would create even bigger queues, and longer query times 
--    for our end-users.

-- 2) To better organize the queries being executed, as they will all be displayed, in the Snowflake Web
--   Console, under a single warehouse.


-- The Warehouses used in production, in order of importance, with their sizes, are: 

    -- PIXEL_WH (XSMALL)
    -- AUDIENCE_WH (LARGE)
    -- ENRICHMENT_WH (SMALL)


-- The Warehouses used for development, analyzing and tests (and for the Tasks running in the background) are:

    -- ANALYST_WH (SMALL)
    -- DATALOADER (LARGE)
    -- DATALOADER_2 (LARGE)
    -- LORENZO_TEST (XSMALL)
    -- DASHBOARD_WH (MEDIUM)
    -- KEYWORD_WH (SMALL) - (this is a legacy warehouse; from what I know, it's not currently being utilized)



-- When loading data, from Amazon S3 or other source, the Warehouse that should be used is the "DATALOADER" Warehouse,
-- for improved performance.

-- Our current Snowflake Account type is Standard, so we don't have access to multi-clustering (the option of having 
-- multiple machines/copies of a warehouse's machine, to run queries in parallel). To compensate for the lack of multi-clustering,
-- you should set your warehouse sizes slightly bigger than normal, as they are going to operate on a "query-by-query" basis (one per time).

-- When creating new Warehouses, ALWAYS REMEMBER to run the appropriate "GRANT USAGE" command on top of the Roles that 
-- will use that Warehouse. If you don't execute this command, the Role and the users which have this role assigned to 
-- won't be able to run queries with that warehouse. 

-- The Role used in production that currently holds the most Privileges/Grants 
-- is PIXEL_AGENT, so the "GRANT" command will probably look like this:

GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE PIXEL_AGENT;

-- To test if the command worked successfully, you can run these commands:

-- Impersonate PIXEL_AGENT role
USE ROLE PIXEL_AGENT; 

-- Check if Warehouse can be used
USE WAREHOUSE <warehouse_name>;

-- Check if queries can be executed with that warehouse:
SELECT * FROM <table_name>;




-- B) Databases, Schemas and Tables (Static)



-- B.1) Currently, our Snowflake Account has four Databases:

-- 1) AUDIENCELAB_INTERNAL_PROD (used for both staging and production audiencelab Apps)

-- 2) FOUR_EYES (also used on both staging and production)

-- 3) TROVO (this is basically a database used for testing; also contains a File Format, "UNIVERSAL_PERSON_FILE_FORMAT", which was/is used on some COPY statements, as will be explained later)

-- 4) STATISTICS (currently empty, this database is to be used for running analytics, using data copied from S3)



-- The only database which is not Transient (failsafe disabled, time-travel reduced) is "AUDIENCELAB_INTERNAL_PROD".
-- The other ones all are Transient, and have had their time-travel disabled. This was done to reduce costs.
-- The commands that should be executed to obtain these cost-effective databases are:

-- Create Transient Database
    CREATE TRANSIENT DATABASE <database_name>;
-- Disable time-travel
    ALTER DATABASE <database_name>
    SET DATA_RETENTION_TIME_IN_DAYS=0;

-- The rest of the queries related to database-creation are found in "2-DATABASES_AND_SCHEMAS", but they 
-- don't stray too far away from what was explained above.



-- B.2) Currently, the Schema configuration of the database has been greatly streamlined, so we only have a few schemas being used with our queries, which are:

-- 1) AUDIENCELAB_INTERNAL_PROD.PUBLIC

-- 2) AUDIENCELAB_INTERNAL_PROD.RAW_DATA

-- 3) FOUR_EYES.PUBLIC

-- 4) TROVO.PUBLIC


-- The rest of the Schemas are related to testing/development, or leftovers from the previous Snowflake Account.






-- B.3) The Tables (Static) used by our queries, with the respective users and warehouses related to these queries, are:


-- 1) AUDIENCELAB_INTERNAL_PROD.PUBLIC.B2B_EXPORT_TROVO (AUDIENCEUSER, AUDIENCE_WH);

-- 2) AUDIENCELAB_INTERNAL_PROD.PUBLIC.B2C_CONSUMER_FULL (AUDIENCEUSER, AUDIENCE_WH);

-- 3) AUDIENCELAB_INTERNAL_PROD.PUBLIC.KEYWORD_PLANNER (STAGINGPIXEL, PIXEL_WH);

-- 4) AUDIENCELAB_INTERNAL_PROD.PUBLIC.TROVO_FEED_RESOLVED (STAGINGPIXEL, PIXEL_WH);

-- 5) AUDIENCELAB_INTERNAL_PROD.PUBLIC.UNIVERSAL_PERSON (AUDIENCEUSER, AUDIENCE_WH)