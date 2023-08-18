-----------
--OBJECTIVE
-----------
/*
THIS IS A COMPARISON OF THE TRIMMINGS AND CODE REQUIRED FOR MANAGING A STREAM&TASK VS DYNAMIC TABLE
WHILE GETTING THE SAME END RESULT. 

ANY TASK WITH THE POTENTIAL FOR BEING REWRITTEN AS A DECLARATIVE QUERY CAN BE FLAGGED AS AN OPPORTUNITY
FOR SIMPLIFYING PIPELINE MANAGEMENT

THE TABLES AND QUERY LOGIC ARE MINIMALISTIC IN ORDER TO PUT THE FOCUS ON THE SYNTACTIC CODE REQUIRED FOR EACH

THE EXAMPLE BUILDS A MATERIALIZED REPORT TABLE ON TOP OF MULTIPLE SOURCE TABLES
HOWEVER, THIS COMPARISON IS NOT LIMITED TO REPORTING TABLES, AND CAN APPLY TO MANY
DATA PIPELINES THAT NEED TO MATERIALIZE THE RESULTS OF A QUERY
*/

-------------
--BASIC SETUP
-------------
--CREATE DUMMY SOURCE TABLES
CREATE OR REPLACE TABLE PRODUCT
(PRODUCT_ID int,
NAME STRING
);

CREATE OR REPLACE TABLE ORDER_LINE
(ORDER_ID INT IDENTITY,
PRODUCT_ID INT,
ORDER_DATE TIMESTAMP_NTZ,
PRICE NUMERIC(10,2),
QUANTITY INT
);

--DUMMY DATA
INSERT INTO PRODUCT (PRODUCT_ID, NAME)
SELECT 201, 'Rubber Duck'
UNION ALL SELECT 202, 'Soap on a Stick'
UNION ALL SELECT 203, 'Soap not on a Stick'
UNION ALL SELECT 204, 'Shower Cap';

INSERT INTO ORDER_LINE (PRODUCT_ID, ORDER_DATE, PRICE, QUANTITY)
SELECT 201 AS PRODUCT_ID, '2023-08-01' AS ORDER_DATE, 9.99 AS PRICE, 2 AS QUANTITY
UNION ALL SELECT 202 AS PRODUCT_ID, '2023-08-01' AS ORDER_DATE, 3.99 AS PRICE, 1 AS QUANTITY
UNION ALL SELECT 202 AS PRODUCT_ID, '2023-08-01' AS ORDER_DATE, 3.99 AS PRICE, 3 AS QUANTITY
UNION ALL SELECT 204 AS PRODUCT_ID, '2023-08-01' AS ORDER_DATE, 4.00 AS PRICE, 1 AS QUANTITY
UNION ALL SELECT 201 AS PRODUCT_ID, '2023-08-04' AS ORDER_DATE, 7.99 AS PRICE, 1 AS QUANTITY;

------------
--BUILD TASK
------------
--CREATE TARGET TABLE
CREATE OR REPLACE TABLE DAILY_PRODUCT_REPORT_TASK
(PRODUCT_NAME STRING,
ORDER_DATE TIMESTAMP_NTZ,
TOTAL_REVENUE NUMERIC(10,2)
);

--CREATE STREAM
CREATE OR REPLACE STREAM ORDER_STRM ON TABLE ORDER_LINE;

--RUN INITAL LOAD BECAUSE STREAM ONLY SEES NEW RECORDS
INSERT INTO DAILY_PRODUCT_REPORT_TASK (PRODUCT_NAME, ORDER_DATE, TOTAL_REVENUE)
SELECT
	P.NAME AS PRODUCT_NAME,
	O.ORDER_DATE AS ORDER_DATE,
	SUM(O.PRICE * O.QUANTITY) AS TOTAL_REVENUE
FROM ORDER_LINE O
JOIN PRODUCT P
ON O.PRODUCT_ID = P.PRODUCT_ID
--NEED TO MAKE SURE WE DON'T BACKFILL RECORDS THAT ARE ON THE STREAM
--BECAUSE THOSE WILL BE HANDLED BY THE TASK
WHERE O.ORDER_ID NOT IN (SELECT ORDER_ID FROM ORDER_STRM)
GROUP BY P.NAME, O.ORDER_DATE;

--CHECK STREAM
SELECT * FROM ORDER_STRM;

--CREATE TASK
--WITH MERGE AND BUSINESS LOGIC FOR HANDLING MERGE CASES
CREATE OR REPLACE TASK ORDER_STRM_TSK
WAREHOUSE = DEV_UKDM_UKP_DATA_ENGINEER_WHS
SCHEDULE = 'USING CRON * * * * * UTC'
WHEN
SYSTEM$STREAM_HAS_DATA('ORDER_STRM')
AS
MERGE INTO DAILY_PRODUCT_REPORT_TASK T
USING (
	SELECT
		P.NAME AS PRODUCT_NAME,
		O.ORDER_DATE AS ORDER_DATE,
		SUM(O.PRICE * O.QUANTITY) AS TOTAL_REVENUE
	FROM ORDER_STRM O
	JOIN PRODUCT P
	ON O.PRODUCT_ID = P.PRODUCT_ID
	GROUP BY P.NAME, O.ORDER_DATE
) S
ON T.PRODUCT_NAME = S.PRODUCT_NAME
AND T.ORDER_DATE = S.ORDER_DATE
WHEN MATCHED
THEN
	--IF THERE ARE NEW ORDERS ON THE SAME DATE AND PRODUCT
	--THEN ADD THE NEW TOTAL TO THE PREVIOUS TOTAL
	UPDATE SET T.TOTAL_REVENUE = T.TOTAL_REVENUE + S.TOTAL_REVENUE
WHEN NOT MATCHED
THEN
	INSERT (PRODUCT_NAME, ORDER_DATE, TOTAL_REVENUE)
	VALUES (S.PRODUCT_NAME, S.ORDER_DATE, S.TOTAL_REVENUE)
;

--START THE TASK SCHEDULE
ALTER TASK ORDER_STRM_TSK RESUME;

--OPTIONAL TO EXECUTE THE TASK NOW
EXECUTE TASK ORDER_STRM_TSK;

--MONITOR TASK HISTORY
select *
from table(information_schema.task_history(
task_name=>'ORDER_STRM_TSK'));

------------------------
-- BUILD DYNAMIC TABLE
------------------------
CREATE OR REPLACE DYNAMIC TABLE DAILY_PRODUCT_REPORT_DYNAMIC
TARGET_LAG = '1 minute'
WAREHOUSE = DEV_UKDM_UKP_DATA_ENGINEER_WHS
AS
SELECT
	P.NAME AS PRODUCT_NAME,
	O.ORDER_DATE AS ORDER_DATE,
	SUM(O.PRICE * O.QUANTITY) AS TOTAL_REVENUE
FROM ORDER_LINE O
JOIN PRODUCT P
ON O.PRODUCT_ID = P.PRODUCT_ID
GROUP BY P.NAME, O.ORDER_DATE;

--OPTIONAL TO TRIGGER INITIAL LOAD OF THE TABLE
ALTER DYNAMIC TABLE DAILY_PRODUCT_REPORT_DYNAMIC REFRESH;

--MONITOR TABLE REFRESH HISTORY
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME = 'DAILY_PRODUCT_REPORT_DYNAMIC';
-----------------------

-----------------
--COMPARE RESULTS
-----------------
SELECT *
FROM DAILY_PRODUCT_REPORT_TASK;

SELECT *
FROM DAILY_PRODUCT_REPORT_DYNAMIC;

--SIMULATE NEW ROW
INSERT INTO ORDER_LINE (PRODUCT_ID, ORDER_DATE, PRICE, QUANTITY)
SELECT 202 AS PRODUCT_ID, '2023-08-01' AS ORDER_DATE, 3.99 AS PRICE, 10 AS QUANTITY;

--OPTIONAL MANUAL REFRESH BOTH TABLES
ALTER DYNAMIC TABLE DAILY_PRODUCT_REPORT_DYNAMIC REFRESH;
EXECUTE TASK ORDER_STRM_TSK;

--COMPARE RESULTS
SELECT *
FROM DAILY_PRODUCT_REPORT_TASK;

SELECT *
FROM DAILY_PRODUCT_REPORT_DYNAMIC;

---------
--CLEANUP
---------
DROP TASK ORDER_STRM_TSK;
DROP TABLE DAILY_PRODUCT_REPORT_TASK;
DROP STREAM ORDER_STRM;
DROP DYNAMIC TABLE DAILY_PRODUCT_REPORT_DYNAMIC;
DROP TABLE ORDER_LINE;
DROP TABLE PRODUCT;