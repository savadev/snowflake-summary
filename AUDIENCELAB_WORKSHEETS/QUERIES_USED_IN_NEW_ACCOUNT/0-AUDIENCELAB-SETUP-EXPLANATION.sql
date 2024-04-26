-- INTRODUCTION


-- If the company ever needs to set up everything in a new Snowflake Account again, the queries shown in the files of this folder 
-- should provide a substantial aid in the process.

-- The purpose of this specific file, on the other hand, is to help with the understanding of the Snowflake-side of the 
-- Audiencelab platform, how it communicates with our backend, what processes are currently 
-- running on our Snowflake Account, and how everything is glued together.


-- OVERVIEW


-- Before going through the steps that should be taken to set up everything anew, in a new account, it is best 
-- to describe the current database configuration, which can be divided between "Static" and "Dynamic" elements.
-- Basically, the most important elements, of both types, are:


-- STATIC ELEMENTS (elements that don't change over time; in other words, elements which don't have tasks, streams or pipes attached to them; also, objects that don't need commands to suspend/start):


-- A) The Warehouses currently being used;

-- B) The Databases, Schemas and Tables currently being used with the platform's queries;

-- C) The Queries which are currently reaching the Account's warehouses;

-- D) Users, Roles and Privileges;

-- E) Other objects (Streams, Pipes, Procedures, File Formats, Stages, Storage Integrations);


-- DYNAMIC ELEMENTS:


-- A) The Tasks being used to maintain the "PREMADE_4EYES_LITE" table (as they need to be suspended/started);

-- B) The "RAW_TOPICS" Table, which is constantly being fed by the "CSV_PIPE", and grows constantly

-- C) The "PREMADE_4EYES_LITE" Table itself, which has rows being INSERTED and DELETED daily (achieved by the "RAW_TOPICS_TASK" and "DELETE_OLD_DATA_TASK" tasks, respectively)




-- Before explaining the Dynamic Elements, we must go through the Static Elements, one by one.




-- STATIC ELEMENTS:


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
    -- KEYWORD_WH (SMALL) - (this is a legacy warehouse; as far as I know, it's not currently being utilized)



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

-- 6) FOUR_EYES.PUBLIC.SHA_TO_UPS (this is a table used as a material; it's not used by any of the production warehouses, but is used with "RAW_TOPICS" to create the PREMADE_4EYES_LITE table, which will be explained later)




-- All of these tables' creation statements can be found in the "9-TABLES(EMPTY-SOME-NEED-STAGES-AND-INTEGRATIONS).sql" file.

-- These tables are created initially empty (no "CREATE TABLE AS SELECT ..." was used), and then filled up with data from s3, using the 
-- queries seen on file "10-FILLING-TABLES-WITH-DATA.sql", which will be explained later.






-- C) Queries reaching our Warehouses

-- All the queries fired by users of our app, in staging/production, are 
-- SELECT queries; in our app, there are no UPDATE/SELECT/INSERT queries. The 
-- only elements that execute INSERT and DELETE queries, in our database, are 
-- the 'RAW_TOPICS_TASK' and 'DELETE_OLD_DATA_TASK' tasks.
-- 
-- The most important SELECT queries (Warehouse - User) reaching our Warehouses are:


-- C.1) AUDIENCE_WH


-- First Query Responsible for Audience Creation (AUDIENCE_WH - AUDIENCEUSER)
SELECT COUNT(*) as COUNT
from (
    SELECT consumer.up_id 
    FROM FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE pa 
    INNER JOIN AUDIENCELAB_INTERNAL_PROD.PUBLIC.CONSUMER_B2C_FULL consumer 
    ON pa.up_id=consumer.up_id 
    WHERE consumer.id IS NOT NULL 
    AND (EXACT_AGE between 20 AND 100) 
    AND SEGMENT IN ('4eyes_105135','4eyes_105147','4eyes_105273','4eyes_105274','4eyes_105471','4eyes_105495','4eyes_105560','4eyes_105871','4eyes_105879','4eyes_105965','4eyes_111669','4eyes_111815','4eyes_105133') 
    AND date >= '2024-04-19' 
    AND date <= '2024-04-25'
    );

-- Second Query Responsible for Audience Creation (AUDIENCE_WH - AUDIENCEUSER)
SELECT * FROM AUDIENCELAB_INTERNAL_PROD.PUBLIC.UNIVERSAL_PERSON 
WHERE UP_ID IN (
    SELECT consumer.up_id 
    FROM FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE pa 
    INNER JOIN AUDIENCELAB_INTERNAL_PROD.PUBLIC.CONSUMER_B2C_FULL consumer 
    ON pa.up_id=consumer.up_id 
    WHERE consumer.id IS NOT NULL 
    AND SEGMENT IN ('4eyes_101119','4eyes_101126','4eyes_101121','4eyes_101122','4eyes_101125','4eyes_101129')  
    AND date >= '2024-04-22' 
    AND date <= '2024-04-26' 
    LIMIT 200000);

-- Query Responsible for B2B Search Creation (AUDIENCE_WH - AUDIENCEUSER)
SELECT FIRST_NAME, LAST_NAME, BUSINESS_EMAIL, PROGRAMMATIC_BUSINESS_EMAILS, 
PERSONAL_EMAIL, JOB_TITLE, SENIORITY_LEVEL, DEPARTMENT, MOBILE_PHONE, 
DIRECT_NUMBER, LINKEDIN_URL, PERSONAL_ADDRESS, PERSONAL_ADDRESS_2, 
PERSONAL_CITY, PERSONAL_STATE, PERSONAL_ZIP, PERSONAL_ZIP4, PROFESSIONAL_ADDRESS, 
PROFESSIONAL_ADDRESS_2, PROFESSIONAL_CITY, PROFESSIONAL_STATE, PROFESSIONAL_ZIP, 
PROFESSIONAL_ZIP4, COMPANY_NAME, COMPANY_DOMAIN, COMPANY_PHONE, PRIMARY_INDUSTRY, 
COMPANY_SIC, COMPANY_NAICS, COMPANY_ADDRESS, COMPANY_ADDRESS_2, COMPANY_CITY, 
COMPANY_STATE, COMPANY_ZIP, COMPANY_ZIP4, COMPANY_LINKEDIN_URL, COMPANY_REVENUE, 
COMPANY_EMPLOYEE_COUNT, BUSINESS_EMAIL_VALIDATION_STATUS, BUSINESS_EMAIL_LAST_SEEN, 
COMPANY_LAST_UPDATED, JOB_TITLE_LAST_UPDATED, LAST_UPDATED 
FROM AUDIENCELAB_INTERNAL_PROD.PUBLIC.B2B_EXPORT_TROVO 
WHERE (COMPANY_SIC LIKE '%8111%') 
LIMIT 5000 
OFFSET 85000;


-- C.2) PIXEL_WH


-- All these queries are Related to the Pixel part of the application:


-- Query #1 (PIXEL_WH - STAGINGPIXEL)
SELECT * FROM PUBLIC.TROVO_FEED_RESOLVED 
WHERE EVENT_DATE >= '2024-01-03T22:32:00.000Z' 
AND SHA256_LOWER_CASE != 'b3f2942c56e24af591f7f944b009f36f40c95fe6f19aa40162758415f39855ae' 
ORDER BY EVENT_DATE ASC 
LIMIT 5000;

-- Query #2 (PIXEL_WH - STAGINGPIXEL)
Select MAX(utc_date) 
AS latestDate 
FROM AUDIENCELAB_INTERNAL_PROD.PUBLIC.KEYWORD_PLANNER;

-- Query #3 (PIXEL_WH - STAGINGPIXEL)
Select DISTINCT(date) 
AS dateList 
FROM FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE 
ORDER By dateList ASC;


-- C.3) ENRICHMENT_WH


-- Query Responsible for Enrichment Creation (ENRICHMENT_WH - ENRICHMENTUSER)
SELECT
    *
FROM
    AUDIENCELAB_INTERNAL_PROD.PUBLIC.UNIVERSAL_PERSON
WHERE
    (
        BUSINESS_EMAIL = 'staylor@ghcr.com'
        OR PROGRAMMATIC_BUSINESS_EMAILS LIKE '%staylor@ghcr.com%'
        OR PERSONAL_EMAIL = 'staylor@ghcr.com'
        OR ADDITIONAL_PERSONAL_EMAILS LIKE '%staylor@ghcr.com%'
    )
    OR (
        BUSINESS_EMAIL = 'stavros@sasmhq.org'
        OR PROGRAMMATIC_BUSINESS_EMAILS LIKE '%stavros@sasmhq.org%'
        OR PERSONAL_EMAIL = 'stavros@sasmhq.org'
        OR ADDITIONAL_PERSONAL_EMAILS LIKE '%stavros@sasmhq.org%'
    )
    OR (
        BUSINESS_EMAIL = 'stasia@2degreesnorth.com'
        OR PROGRAMMATIC_BUSINESS_EMAILS LIKE '%stasia@2degreesnorth.com%'
        OR PERSONAL_EMAIL = 'stasia@2degreesnorth.com'
        OR ADDITIONAL_PERSONAL_EMAILS LIKE '%stasia@2degreesnorth.com%'
    );




-- D) Users, Roles and Privileges 



-- In order to be able to run any SELECT queries on our tables, a number of Privileges must 
-- be granted to our roles. 

-- The correct order is "PRIVILEGE > ROLE > USER". Privileges are GRANTed to Roles, which are then, themselves, 
-- GRANTed to Users.

-- To be able to grant any permissions to our users, it is preferable to use the Admin Roles "ACCOUNTADMIN" or 
-- "SYSADMIN"

-- The main Role used in our Snowflake Account is "PIXEL_AGENT", which has already been granted to the
-- AUDIENCEUSER, STAGINGPIXEL and ENRICHMENTUSER Users.

-- The "PIXEL_AGENT" Role already has access to the PIXEL_WH, AUDIENCE_WH and ENRICHMENT_WH warehouses. It also has 
-- access to the AUDIENCELAB_INTERNAL_PROD, TROVO and FOUR_EYES databases.

-- If PIXEL_AGENT Role accidentally loses its privileges, or if eventually a new Snowflake Account needs 
-- to be set up, documents "6-USERS_AND_ROLES.sql", "7-GRANT_PRIVILEGES_TO_ROLES.sql" and "8-ASSIGN_ROLES_TO_USERS.sql" can 
-- be used to ensure a smooth transition.

-- Remember that, for a user to be able to run queries on a given table, he/she needs a role with access 
-- to both the table (with usage on the database/schema) AND a warehouse, which will be used to execute the queries.

-- It is also useful to Grant the Privilege of "SELECT ON FUTURE TABLES IN SCHEMA" to our roles, if we wish 
-- to create any additional tables in one of our schemas, in the future (because this permission will not be 
-- granted automatically, if we don't use this command).

-- If a user doesn't have permission to access a SQL object or to use a Warehouse, Snowflake will warn him 
-- that the object/warehouse doesn't exist, even if it does.

-- A good example of how this access is given to the users in our backend can be seen with this snippet (taken from documents 6-8 of this folder):

-- Create Empty Role
CREATE ROLE PIXEL_AGENT;

-- Create Empty User
CREATE USER AUDIENCEUSER;

-- Grant Database Usage to Role (USAGE + SELECT privileges are needed, to run queries on the tables)
GRANT USAGE ON DATABASE AUDIENCELAB_INTERNAL_PROD TO ROLE PIXEL_AGENT;

-- Grant Schema Usage to Role
GRANT USAGE ON SCHEMA AUDIENCELAB_INTERNAL_PROD.PUBLIC TO ROLE PIXEL_AGENT;

-- Grant Select to Role
GRANT SELECT ON ALL TABLES IN SCHEMA AUDIENCELAB_INTERNAL_PROD.PUBLIC TO ROLE PIXEL_AGENT;

-- Grant Select on All Future Tables, to Role
GRANT SELECT ON FUTURE TABLES IN SCHEMA AUDIENCELAB_INTERNAL_PROD.PUBLIC TO ROLE PIXEL_AGENT;

-- Grant Usage on Warehouse, to Role (to be able to execute queries)
GRANT USAGE ON WAREHOUSE AUDIENCE_WH TO ROLE PIXEL_AGENT;

-- Grant PIXEL_AGENT Role to AUDIENCEUSER User (currently empty):
GRANT ROLE PIXEL_AGENT TO USER AUDIENCEUSER;

-- Check if queries can be executed with that role:
USE ROLE PIXEL_AGENT;
SELECT * FROM AUDIENCELAB_INTERNAL_PROD.PUBLIC.UNIVERSAL_PERSON LIMIT 100;





-- E) Other Objects





-- This section can be quite long, because it is connected to the topic of the Dynamic Elements.

-- The other objects that we have in our Snowflake Account are:





-- E.1) Streams


    -- More information about Streams can be found in the bookmark M23 of the "COURSE-REVIEW.sql" document.

    -- Streams can be summed up as objects whose purpose is to capture 
    -- whatever changes are applied to a given table (CDC,
    -- Change Data Capture). They are always created on 
    -- top of existing tables.

    -- In the case of our Snowflake Account, there is only a single Stream object currently being 
    -- used, which is the "FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM".

    -- What it does, essentially, is the capturing of any data changes (INSERTS, in this case) 
    -- that occur on the "FOUR_EYES.PUBLIC.RAW_TOPICS" table.

    -- The RAW_TOPICS_STREAM creation statement is this one:

        CREATE STREAM FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM 
        ON TABLE RAW_TOPICS 
        append_only = true;

    -- This stream object is used in a combo with the "CSV_PIPE", which dumps data from s3
    -- into the "RAW_TOPICS" Table, daily. This dumped data is captured by this 
    -- stream, which keeps holding the data until the moment it is used. This data 
    -- can be used only once; after being used once (in a INSERT statement), the 
    -- data inside of the stream is deleted.

    -- In the case of our Snowflake App, the object which uses this stream, emptying 
    -- its contents, is the "RAW_TOPICS_TASK" (which consumes the stream's data, by inserting into the PREMADE_4EYES_LITE Table).



-- E.2) Pipes

    -- On our Snowflake Account, there is only a single pipe being used,
    -- and it is related to the maintenance of the PREMADE_4EYES_LITE Table,
    -- along with the Stream object mentioned above and the RAW_TOPICS_TASK and
    -- DELETE_OLD_DATA_TASK Tasks.

    -- The complete identifier of the pipe being used is "FOUR_EYES.PUBLIC.CSV_PIPE"

    -- The main purpose of pipes is "continuous"/automatic 
    -- data copying in your tables, data extracted from uploaded
    -- files.

    -- With Pipes, for every file appearing in your bucket,
    -- a notification is sent, by AWS S3, to the Snowpipe 
    -- Service (serverless) - once this serverless loader
    -- detects that notification, it will identify the 
    -- new files that were uploaded, and then will load 
    -- their contents into your Snowflake Table.

    -- Basically, for every .csv file dropped into the 's3://audiencelab-4eyes/AIGDS/'
    -- S3 location/stage, Snowflake automatically attempts to copy the file's contents 
    -- into a table (in this case, the RAW_TOPICS Table).

    -- When we create a Pipe, we can also apply some transformations to the data 
    -- that is getting inserted/copied into the table, something that can be observed in the creation 
    -- statement of the CSV_PIPE (the "DATE" field's value is constructed from the 
    -- filename of the .csv file):

        CREATE OR REPLACE PIPE FOUR_EYES.PUBLIC.CSV_PIPE 
        auto_ingest=true 
        AS COPY INTO FOUR_EYES.PUBLIC.RAW_TOPICS
        FROM (
            SELECT 
            T.$1 AS "SHA256_LC_HEM",
            T.$3 AS "TOPIC",
            TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value, via value transformation
            FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV AS T
        );
    
    -- More information about the pipes can be found in bookmark 'M16 - Snowpipe' of the "COURSE-REVIEW.sql" document.



-- E.3) Procedures


    -- On our Snowflake Account, there is only a single procedure being used,
    -- which is "FOUR_EYES.PUBLIC.DELETE_OLD_DATA". What this procedure does is 
    -- the execution of a DELETE statement on the PREMADE_4EYES_LITE Table,
    -- targetting all the data older than one week in the past. This is done to maintain
    -- the size of the table, so that it doesn't grow too much and so that the Audience 
    -- creation queries don't become much slower. As a result, the PREMADE_4EYES_LITE Table
    -- always maintains a number of 20-25 billion rows, and about 40-50GB of space.

    -- This Procedure is currently connected to the "FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK",
    -- which is set to simply execute it every day, at 2AM CST.

    -- The code which was used to create the procedure is the following:

        CREATE OR REPLACE PROCEDURE FOUR_EYES.PUBLIC.DELETE_OLD_DATA()
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        EXECUTE AS CALLER
        AS 
        '
        var currentDate = new Date();
        var sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(currentDate.getDate() - 7);

        var formattedDate = sevenDaysAgo.toISOString().split(''T'')[0];

        var sql_command = `DELETE FROM FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE
                            WHERE DATE < ''${formattedDate}''`;
        try {
            
        var stmt = snowflake.createStatement({sqlText: sql_command});
        var rs = stmt.execute();
        return ''Old data successfully deleted.'';
        
        } catch (err) {
            return ''Error deleting old data: '' + err.message;
        }
        ';

    -- To execute the procedure (without relying on the task, for testing purposes), one must call like this:
    CALL FOUR_EYES.PUBLIC.DELETE_OLD_DATA();




-- E.4) File Formats


    -- A File Format, named (object) or not, always needs 
    -- to be specified in your COPY command. File Formats are typically connected to stages,
    -- to then be used with COPY commands.

    -- The greatest advantage of the File Format objects is 
    -- that it does not matter how many COPY commands you have,
    -- if you change the File Format that is registered to all of them,
    -- the changes's effects will be applied to all of the commands as well.

    -- The most common properties used with CSV File Formats are 
    -- RECORD_DELIMITER, SKIP_HEADER and FIELD_DELIMITER.

    -- More information about File Formats can be found on bookmark M11 (COPY preparations) of the "COURSE-REVIEW.sql" document.

    -- In our Snowflake Account, there were a number of File Formats 
    -- that were created to manage the COPY of the data from the s3 
    -- backup (.csv and .json files) into our current account's Snowflake 
    -- Tables.

    -- What File Format must be used with a COPY command depends on the use-case
    -- and on the complexity of the data contained in your files (if the fields contain ; " \n and other symbols, the file format must be adapted).

    -- For copying most of the data present in s3 into our current tables, the 
    -- File Format that was used was "TROVO.PUBLIC.UNIVERSAL_PERSON_FILE_FORMAT_2".
    -- This File Format was incorporated in the "FOUR_EYES.PUBLIC.AUDIENCELAB_BACKUP_STAGE",
    -- and was used to basically copy all the Tables' backed-up .csv data in s3 into our 
    -- current account's tables.

    -- The Creation statement for the file format used on the backups was this one:
        CREATE FILE FORMAT TROVO.PUBLIC.UNIVERSAL_PERSON_FILE_FORMAT_2
        TYPE=CSV
	    SKIP_HEADER = 0
	    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	    NULL_IF = ('', 'null', 'NULL')
	    COMPRESSION = GZIP;


    -- However, that is not the only File Format that is important; we also have the "FOUR_EYES_CSV_FORMAT" and "FOUR_EYES_JSON_FORMAT",
    -- both are used to maintain the PREMADE_4EYES_LITE Table, and are currently connected to the "FOUR_EYES_STAGE_CSV" and "FOUR_EYES_STAGE_JSON",
    -- respectively. 

    -- These File Formats were defined like this:

    -- CSV
    CREATE OR REPLACE FILE FORMAT FOUR_EYES.PUBLIC.FOUR_EYES_CSV_FORMAT
    TYPE=CSV,
    FIELD_DELIMITER=',',
    SKIP_HEADER=1,
    NULL_IF=('NULL', 'null')
    COMPRESSION=gzip;

    -- JSON
    CREATE OR REPLACE FILE FORMAT FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT
    TYPE=JSON,
    COMPRESSION=gzip;




-- E.5) Stages 



    -- Stages are Snowflake Objects used to COPY (COPY command) data from some place 
    -- into another. In the context of our Snowflake Account, the data is 
    -- copied from S3 into our Snowflake Tables. Different stages were used 
    -- for transferring the data from the previous Snowflake Account into the 
    -- current one. What happened then was "OLD_SNOWFLAKE > S3 > NEW_SNOWFLAKE", 
    -- basically.

    -- Snowflake supports a lot of different file types with file formats, but the most used ones 
    -- are .csv, .json and Parquet. 

    -- We can import files into snowflake and export files into other cloud providers,
    -- using stages and the COPY command.

    -- The stage that was used the most for setting up the new Snowflake Account was 
    -- the "FOUR_EYES.PUBLIC.AUDIENCELAB_BACKUP_STAGE", which uses the "UNIVERSAL_PERSON_FILE_FORMAT_2" File Format.

    -- The S3 location targeted by that stage was "s3://audiencelab-4eyes/audiencelab_backup/",
    -- which now contains backups of the tables used on our previous account.

    -- Inside of the "audiencelab_backup" folder, in the audiencelab-4eyes bucket,
    -- we have folders for each of the tables that were kept in the new Snowflake Account.

    -- These folders can be used if the data currently stored in our Snowflake Account gets compromised; the 
    -- data can be copied using the "FOUR_EYES.PUBLIC.AUDIENCELAB_BACKUP_STAGE" Stage, the only tables that 
    -- need some specific care are "B2B_EXPORT_TROVO" (which needs the .json format to be specified on the file format 
    -- of the COPY command) and "PREMADE_4EYES_LITE" (which needs the whole stream-task-procedure-pipe setup).


    -- The folders (and backups) are:

    -- b2b_export_trovo

    -- consumer_b2c_full

    -- keyword_planner

    -- premade_4eyes_lite (old data, shouldn't be used anymore, as it was only used for that single backup process)

    -- premade_taxonomy (wasn't copied over, judged unecessary)

    -- sha_to_ups

    -- st_consumer (empty, data wasn't copied because was too big, and judged unecessary)

    -- trovo_feed_resolved

    -- universal_person

    
    -- All of these folders contain .csv backups of the tables of the previous Snowflake Account,
    -- except for "b2b_export_trovo", which contains .json files (.json format was used, in this case, because 
    -- .csv was producing too many parsing errors when trying to copy the data from the Snowflake Table into 
    -- S3).


    -- As for the statements used for creating the stages, they were:


    -- Used for creating the most-used stage, which was used for recovering the backed-up Table data, from the previous Snowflake account (used with all COPY statements, except for the b2b_export_trovo one)
    CREATE OR REPLACE STAGE FOUR_EYES.PUBLIC.AUDIENCELAB_BACKUP_STAGE
    url='s3://audiencelab-4eyes/audiencelab_backup/'
    STORAGE_INTEGRATION=FOUR_EYES_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='TROVO.PUBLIC.UNIVERSAL_PERSON_FILE_FORMAT_2'
    );

    -- Used for creating the stage used to fill up the RAW_TOPICS Table, daily (Which then gets used to fill up the PREMADE_4EYES Table, daily):
    CREATE OR REPLACE STAGE FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV
    url='s3://audiencelab-4eyes/AIGDS/'
    STORAGE_INTEGRATION=FOUR_EYES_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='FOUR_EYES.PUBLIC.FOUR_EYES_CSV_FORMAT'
    );

    -- Used for creating the stage used to fill up the SHA_TO_UPS Table (this stage was used only once, to fill the Static SHA_TO_UPS Table, but must be kept, as a reference)
    CREATE OR REPLACE STAGE FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_JSON
    url='s3://audiencelab-4eyes/hem_upid/'
    STORAGE_INTEGRATION=FOUR_EYES_INTEGRATION
    FILE_FORMAT=(
        FORMAT_NAME='FOUR_EYES.PUBLIC.FOUR_EYES_JSON_FORMAT'
    );



-- E.6) Storage Integrations


    -- Currently, our Snowflake Account only has a single Storage Integration object,
    -- which is needed to make all the current stages work. The Storage Integration's identifier 
    -- is "FOUR_EYES_INTEGRATION", and it is currently connected to the audiencelab-4eyes bucket.

    -- The Storage Integration's creation code was this one:

    CREATE OR REPLACE STORAGE INTEGRATION FOUR_EYES_INTEGRATION
    TYPE=EXTERNAL_STAGE
    STORAGE_PROVIDER=S3
    ENABLED=TRUE
    STORAGE_AWS_ROLE_ARN='' -- a "snowflake" dedicated IAM user is needed, in AWS, to utilize this value
    STORAGE_ALLOWED_LOCATIONS=('s3://audiencelab-4eyes/');

    -- The STORAGE_AWS_ROLE_ARN must be filled with a dedicated IAM user in AWS,
    -- and the Snowflake Object and s3 bucket must be connected. The steps that 
    -- need to be taken to establish this connection are described in bookmark M11 of the COURSE-REVIEW.sql document.
    -- The current Storage Integration object is enough to maintain our current tables, but if 
    -- we need to import/COPY data from other buckets, a new Storage Integration object will need 
    -- to be created.






-- DYNAMIC ELEMENTS:


    -- A) Tasks


    -- On our Snowflake account, there are two tasks being used:
    -- "FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK" and "FOUR_EYES.PUBLIC.RAW_TOPICS_TASK". 
    -- Both are essential to maintain the data on the "PREMADE_4EYES_LITE" table (which 
    -- is used mainly for Audience Creation). One task (DELETE_OLD_DATA_TASK) is responsible for deleting data older
    -- than one week in the past, and the other (RAW_TOPICS_TASK) is responsible for feeding the table with data 
    -- from the last day, the most recent data received in s3.

    -- The tasks can be considered Dynamic Elements because they can be suspended/started at will,
    -- using SQL commands.

    -- The first task, "DELETE_OLD_DATA_TASK", is very simple, as the only thing it does 
    -- is execute the "DELETE_OLD_DATA" procedure, every day, at 2 AM CST.

    -- Its code is the following:
       CREATE TASK FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK
       WAREHOUSE=ANALYST_WH
       SCHEDULE='USING CRON 0 2 * * * America/Chicago'
       COMMENT='Task to delete data from PREMADE_4EYES_LITE table older than 7 days from the current runtime'
       AS CALL FOUR_EYES.PUBLIC.DELETE_OLD_DATA();
    

    -- The second task, "RAW_TOPICS_TASK", inserts new, transformed rows (JOIN statement between RAW_TOPICS_STREAM and SHA_TO_UPS) on the "PREMADE_4EYES_LITE" table,
    -- every 6 hours, but only if it detects new data on the "RAW_TOPICS_STREAM" (using the "SYSTEM$STREAM_HAS_DATA()" condition).

    -- Its code:
       CREATE TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK
       WAREHOUSE=DATALOADER
       SCHEDULE='360 MINUTES'
       USER_TASK_TIMEOUT_MS=36000000
       WHEN SYSTEM$STREAM_HAS_DATA('FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM')
       AS BEGIN
   --  Perform the join operation and insert into target table
                INSERT INTO FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE ("DATE", "SEGMENT", "UP_ID")
                SELECT
                    RT.DATE AS "DATE",
                    RT.TOPIC AS "SEGMENT",
                    F.value::string AS up_id
                FROM
                    "FOUR_EYES"."PUBLIC"."RAW_TOPICS_STREAM" AS RT
                INNER JOIN
                    "FOUR_EYES"."PUBLIC"."SHA_TO_UPS" AS SHA_TO_UPS
                    ON SHA_TO_UPS."SHA256_LC_HEM" = RT."SHA256_LC_HEM"
                CROSS JOIN
                    LATERAL FLATTEN(input => SHA_TO_UPS.UP_IDS) F;
          END;


   -- These tasks, when created, initially stay in a suspended state. To start/suspend
   -- a given task, we must execute these statements:

      ALTER TASK FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK RESUME;
      ALTER TASK FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK SUSPEND;

      ALTER TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK RESUME;
      ALTER TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK SUSPEND;




    -- B) RAW_TOPICS Table




    -- This Table's full identifier is FOUR_EYES.PUBLIC.RAW_TOPICS.


    -- This Table exists mainly to be used as a material for filling up 
    -- the PREMADE_4EYES_LITE Table, daily.


    -- This Table was initially created empty, using this command:
       
       CREATE TRANSIENT TABLE FOUR_EYES.PUBLIC.RAW_TOPICS (
       SHA256_LC_HEM VARCHAR(16777216),
       TOPIC VARCHAR(16777216),
       DATE DATE
       );


    -- This Table is considered a "Dynamic Element" because it is constantly 
    -- receiving new rows, transformed data, via the "CSV_PIPE" Object, mentioned before, at the E.2 bookmark:
       
       CREATE PIPE FOUR_EYES.PUBLIC.CSV_PIPE 
       auto_ingest=true 
       AS COPY INTO FOUR_EYES.PUBLIC.RAW_TOPICS
       FROM (
            SELECT 
            T.$1 AS "SHA256_LC_HEM",
            T.$3 AS "TOPIC",
            TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value, via value transformation
            FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV AS T
        );
    

    -- As seen before, at the E.1 bookmark, this RAW_TOPICS table is also targeted by a Stream Object,
    -- which captures each new row that gets inserted into it:

        CREATE STREAM FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM 
        ON TABLE RAW_TOPICS 
        append_only = true;


    -- Finally, this Stream Object itself is targeted by a Task, the "RAW_TOPICS_TASK", which
    -- ends up inserting the Stream's data, JOINed with the SHA_TO_UPS (Static) Table, into 
    -- the FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE table, with this code, already seen before:

       CREATE TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK
       WAREHOUSE=DATALOADER
       SCHEDULE='360 MINUTES'
       USER_TASK_TIMEOUT_MS=36000000
       WHEN SYSTEM$STREAM_HAS_DATA('FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM')
       AS BEGIN
   --  Perform the join operation and insert into target table
                INSERT INTO FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE ("DATE", "SEGMENT", "UP_ID")
                SELECT
                    RT.DATE AS "DATE",
                    RT.TOPIC AS "SEGMENT",
                    F.value::string AS up_id
                FROM
                    "FOUR_EYES"."PUBLIC"."RAW_TOPICS_STREAM" AS RT
                INNER JOIN
                    "FOUR_EYES"."PUBLIC"."SHA_TO_UPS" AS SHA_TO_UPS
                    ON SHA_TO_UPS."SHA256_LC_HEM" = RT."SHA256_LC_HEM"
                CROSS JOIN
                    LATERAL FLATTEN(input => SHA_TO_UPS.UP_IDS) F;
          END;



        

    -- C) PREMADE_4EYES Table


          

    -- This Table's full identifier is FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE.

    -- This Table's main purpose is to be used to create Audiences, both premades
    -- and keywords.

    -- This Table was originally created empty, with this command:
       CREATE TRANSIENT TABLE FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE (
       DATE DATE,
       SEGMENT VARCHAR(16777216),
       UP_ID VARCHAR(16777216)
       );

    -- This Table is considered Dynamic because it needs a number of objects to be created 
    -- and maintained.

    -- The two Tasks which constantly maintain this Table are "FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK"
    -- and "FOUR_EYES.PUBLIC.RAW_TOPICS". One deletes rows older than 7 days, the other feeds in rows 
    -- constructed from a SELECT JOIN statement between "RAW_TOPICS_STREAM" (with the latest captured
    -- raw_topic data, obtained from the CSV_PIPE) and "SHA_TO_UPS".

    -- Essentially, our PREMADE_4EYES_LITE Table is connected/related to these two Tasks, which in turn
    -- are connected to other objects (the RAW_TOPICS_STREAM Stream, the CSV_PIPE Pipe and the SHA_TO_UPS Table).

    -- There is also an useful Miro Board which illustrates the relation between each table, and 
    -- how this table gets constructed (see first part, "SCHEMA ORGANIZATION"): https://miro.com/app/board/uXjVN2sDF7o=/