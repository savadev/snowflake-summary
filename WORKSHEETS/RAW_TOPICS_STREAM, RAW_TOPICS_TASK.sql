// 1) Create Stream to Capture Data Changes on Raw Topics Table:
CREATE OR REPLACE STREAM FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM 
ON TABLE FOUR_EYES.PUBLIC.RAW_TOPICS
APPEND_ONLY=TRUE;


// Stream will capture data changes in table RAW_TOPICS. RAW_TOPICS_STREAM

// 2) Create Task

// Now we'll need a task which will, each time there's a change on the RAW_TOPICS table (append), 
// INSERT  data   produced  by a SELECT  with join between the RAW_TOPICS_STREAM and the SHA_TO_UPS table:
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



// 3) Resume/Start Created Task
ALTER TASK FOUR_EYES.PUBLIC.RAW_TOPICS_TASK RESUME;




select *
from table(four_eyes.information_schema.copy_history(TABLE_NAME=>'RAW_TOPICS', START_TIME=> DATEADD(days, -2, CURRENT_TIMESTAMP())));






// Check RAW_TOPICS_STREAM stream:
SELECT * FROM FOUR_EYES.PUBLIC.RAW_TOPICS_STREAM;




SHOW TASKS;





