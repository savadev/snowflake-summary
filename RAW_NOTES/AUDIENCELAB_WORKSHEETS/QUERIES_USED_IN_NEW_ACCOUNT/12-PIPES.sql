-- Set up the PIPES that were used with the previous account (we need "FOUR_EYES_STAGE_CSV" Stage): 
CREATE OR REPLACE PIPE FOUR_EYES.PUBLIC.CSV_PIPE 
    auto_ingest=true 
    AS COPY INTO FOUR_EYES.PUBLIC.RAW_TOPICS
    FROM (
        SELECT 
        T.$1 AS "SHA256_LC_HEM",
        T.$3 AS "TOPIC", -- changed, before it was T.$2
        TO_DATE(SUBSTR(METADATA$FILENAME, 10, 8), 'YYYYMMDD') AS DATE  -- Get the DATE Value
        FROM @FOUR_EYES.PUBLIC.FOUR_EYES_STAGE_CSV AS T
    );




DESC PIPE FOUR_EYES.PUBLIC.CSV_PIPE;