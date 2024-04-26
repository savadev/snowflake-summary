-- Raw_topics task:
CREATE OR REPLACE TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK
    WAREHOUSE='DATALOADER'
    -- SCHEDULE='USING CRON 0 17 * * * America/Chicago'
    SCHEDULE='360 MINUTES'
    USER_TASK_TIMEOUT_MS=36000000
WHEN 
    SYSTEM$STREAM_HAS_DATA('FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM')
    AS 
            BEGIN
-- Perform the join operation and insert into target table
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

-- Start FOUR_EYES.PUBLIC.RAW_TOPICS_TASK:
ALTER TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK RESUME;




-- Daily PREMADE_4EYES_LITE Delete Rows task (uses "DELETE_OLD_DATA()" task):
create or replace task FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK
	warehouse=ANALYST_WH
	schedule='USING CRON 0 2 * * * America/Chicago'
	COMMENT='Task to delete data from PREMADE_4EYES_LITE table older than 7 days from the current runtime'
	as CALL FOUR_EYES.PUBLIC.DELETE_OLD_DATA();


-- Start FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK:
ALTER TASK FOUR_EYES.PUBLIC.DELETE_OLD_DATA_TASK RESUME;