SELECT count(*) as count
from (
        SELECT consumer.*
        FROM premade_audiences pa
            INNER JOIN public.consumer_b2b_template consumer ON pa.consumer_id = consumer.consumer_id
        WHERE consumer.CONSUMER_ID IS NOT NULL
            AND arrays_overlap(array_construct('1jozgx3gp68'), pa.segment_list)
            AND utc_date > '2023-06-28'
    );