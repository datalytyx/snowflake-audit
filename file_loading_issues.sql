-- Setup and context
USE ROLE ACCOUNTADMIN;
USE SCHEMA SNOWFLAKE.ACCOUNT_USAGE;
USE WAREHOUSE MY_WAREHOUSE_NAME;

-- Read file loading history, add some categories e.g. for filesize, add some rules for spotting issues
WITH LABELLING AS (
    SELECT
        CASE 
            WHEN POSITION('.', FILE_NAME) = 0 THEN NULL
            ELSE SPLIT_PART(FILE_NAME, '.', -1) 
            END AS FILE_TYPE,
        CASE 
            WHEN FILE_SIZE<10000000 THEN '<10MB'
            WHEN FILE_SIZE<100000000 THEN '10MB-100MB'
            WHEN FILE_SIZE<500000000 THEN '100MB-500MB'
            ELSE '500MB+' 
            END AS FILE_SIZE_BUCKETS,
        CASE
            WHEN LOWER(FILE_TYPE) IN ('gz', 'gzip', 'bz2', 'brotli', 'zstd', 'zz') THEN 'Compressed'
            WHEN LOWER(FILE_TYPE) IN ('avro', 'csv', 'json', 'tsv', 'xlsx') THEN 'Uncompressed'
            WHEN LOWER(FILE_TYPE) IN ('zip') THEN 'Unsupported'
            ELSE 'Unknown'
            END AS IS_COMPRESSED,
        CASE WHEN FILE_SIZE >= 500000000 THEN 'Too large (maybe)' END AS PI_FS,
        CASE WHEN IS_COMPRESSED = 'Uncompressed' THEN 'Could be compressed' END AS PI_IC,
        CASE WHEN IS_COMPRESSED = 'Unsupported' THEN 'Unsupported type' END AS PI_TY
    FROM COPY_HISTORY
)
-- Summarise the results
SELECT
    FILE_TYPE,
    FILE_SIZE_BUCKETS,
    IS_COMPRESSED,
    COUNT(*) AS FILE_COUNT,
    -- This is just a fancy way of concatenating the different strings which adds a separator but doesn't fail when a null value is passed in
    ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(PI_FS, PI_IC, PI_TY)), ', ') AS POTENTIAL_ISSUES
FROM LABELLING
GROUP BY FILE_TYPE, FILE_SIZE_BUCKETS, IS_COMPRESSED, POTENTIAL_ISSUES
ORDER BY FILE_COUNT DESC;
