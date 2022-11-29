-- Setup and context
USE ROLE SYSADMIN;
USE SCHEMA SNOWFLAKE.ACCOUNT_USAGE;
USE WAREHOUSE MY_WAREHOUSE_NAME;

-- WHs which suffer from spilling
WITH QUERY_CATEGORISATION AS (
    SELECT 
        USER_NAME, 
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE > 0 
            THEN 1 
            ELSE 0 
            END AS SPILLED_LOCAL,                                       -- How many queries spilled to local disk
        CASE WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 
            THEN 1 
            ELSE 0 
            END AS SPILLED_REMOTE,                                      -- How many queries spilled to remote disk
        CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE + BYTES_SPILLED_TO_REMOTE_STORAGE) > 0 
            THEN 1 
            ELSE 0 
            END AS SPILLED_EITHER                                       -- How many queries spilled to either local or remote
    FROM QUERY_HISTORY
    WHERE 
        TOTAL_ELAPSED_TIME > 1000                                       -- Queries which ran for <1s are unlikely to have spilled
        AND WAREHOUSE_SIZE IS NOT NULL                                  -- We can exclude queries with no warehouse e.g. DDL
        AND
            (BYTES_SPILLED_TO_LOCAL_STORAGE > 0
            OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
      ),
-- How many queries in total so we can compare spill:non-spill
QUERY_TOTALS AS (
    SELECT 
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        COUNT(QUERY_ID) AS QUERY_COUNT
    FROM QUERY_HISTORY
    WHERE WAREHOUSE_SIZE IS NOT NULL
    GROUP BY WAREHOUSE_NAME, WAREHOUSE_SIZE
    )
-- Summarise and produce output
SELECT 
    USER_NAME, 
    C.WAREHOUSE_NAME,
    C.WAREHOUSE_SIZE,
    T.QUERY_COUNT,
    COUNT(C.SPILLED_EITHER) AS QUERIES_SPILLED,                         -- Count of queries that spilled
    ROUND(100*C.SPILLED_EITHER/T.QUERY_COUNT,2) AS PERCENTAGE_SPILLED,  -- Percentage of queries that spilled
    SUM(SPILLED_LOCAL) as "QUERIES WITH LOCAL SPILLING (bad)",          -- How many spilled locally
    SUM(SPILLED_REMOTE) as "QUERIES WITH REMOTE SPILLING (very bad)"    -- How many spilled remotely
FROM QUERY_CATEGORISATION C
LEFT JOIN QUERY_TOTALS T
    ON  C.WAREHOUSE_NAME = T.WAREHOUSE_NAME                             -- Join to the overall query stats to compare the number spilling with the total number
    AND C.WAREHOUSE_SIZE = T.WAREHOUSE_SIZE                             -- We join on both size and name so we can handle cases where a warehouse has been resized, and consider those sizes separately
GROUP BY USER_NAME, C.WAREHOUSE_NAME, C.WAREHOUSE_SIZE, T.QUERY_COUNT, PERCENTAGE_SPILLED
ORDER BY QUERIES_SPILLED DESC
;
