SELECT count(*) as count from (SELECT consumer.up_id FROM four_eyes.public.PREMADE_4EYES_LITE pa INNER JOIN AUDIENCELAB_INTERNAL_PROD.PUBLIC.CONSUMER_B2C_FULL consumer ON pa.up_id=consumer.up_id WHERE consumer.up_id IS NOT NULL AND SEGMENT IN ('4eyes_105106')  AND date = '2024-02-10');



// 10, 11, 12, 13, 14, 15, 16, 17, 18


// Resume PREMADE_4EYES Table Re-creation task:


// 1) Show TASKS:
USE DATABASE FOUR_EYES;
SHOW TASKS;

// 2) Describe Task:
DESCRIBE TASK PREMADE_4EYES_TASK;

// 3) Resume Task:
ALTER TASK FOUR_EYES.PUBLIC.PREMADE_4EYES_TASK RESUME;