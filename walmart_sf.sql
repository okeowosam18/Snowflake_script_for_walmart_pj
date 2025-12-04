create or replace warehouse WALMART_WH;

use warehouse WALMART_WH;
use role ACCOUNTADMIN;

create or replace database WALMART_DB;
create or replace SCHEMA WALMART_DB.BRONZE;


create or replace table raw_data.department(
    Store INTEGER,
    Dept INTEGER,
    Date DATE,
    Weekly_Sales DECIMAL(10, 2),
    IsHoliday BOOLEAN
);

create or replace table raw_data.fact(
    Store INTEGER,
    Date DATE,
    Temperature DECIMAL(5, 2),
    Fuel_Price DECIMAL(6, 3),
    MarkDown1 DECIMAL(10, 2),
    MarkDown2 DECIMAL(10, 2),
    MarkDown3 DECIMAL(10, 2),
    MarkDown4 DECIMAL(10, 2),
    MarkDown5 DECIMAL(10, 2),
    CPI DECIMAL(12, 7),
    Unemployment DECIMAL(5, 3),
    IsHoliday BOOLEAN
);

SHOW INTEGRATIONS;

CREATE OR REPLACE STORAGE INTEGRATION s3_walmart_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::535355705399:role/walmart-data-snowpipe'
  STORAGE_ALLOWED_LOCATIONS = ('s3://walmart-s3-data-filez/');

-- Describe integration to get the Snowflake IAM User and External ID
DESC STORAGE INTEGRATION s3_walmart_integration;

CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE;

create or replace stage walmart_raw_stage
    storage_integration = s3_walmart_integration
    URL = 's3://walmart-s3-data-filez/'
    FILE_FORMAT = my_csv_format
    directory = (ENABLE = true 
    AUTO_REFRESH = TRUE)
    FILE_FORMAT = my_csv_format;

list @walmart_raw_stage; 

ALTER STAGE WALMART_RAW_STAGE REFRESH;

SELECT * FROM DIRECTORY (@walmart_raw_stage);

SELECT RELATIVE_PATH FROM DIRECTORY (@walmart_raw_stage);

CREATE OR REPLACE TABLE FILE_LOAD_LOG(
    FILE_NAME STRING,
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE OR REPLACE PROCEDURE DYNAMIC_TABLE_LOAD()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    file_path STRING;
    folder_name STRING;
    tablename STRING;
    file_name STRING;
    full_stage_path STRING;
    record_count INTEGER;
    create_sql STRING;
    copy_sql STRING;
    file_cursor CURSOR FOR SELECT RELATIVE_PATH FROM DIRECTORY(@walmart_raw_stage);
BEGIN
    -- loop through each file record returned by the directory table function
    FOR file_rec IN file_cursor DO
        file_path := file_rec.RELATIVE_PATH;
        
        -- Extract the folder name from the file path
        folder_name := SPLIT_PART(file_path, '/', 1);
        
        -- Extract the file names to check which files have been processed
        file_name := SPLIT_PART(file_path, '/', 2);
        
        -- Check if the file has already been processed
        SELECT COUNT(1) INTO :record_count 
        FROM FILE_LOAD_LOG 
        WHERE FILE_NAME = :file_path;
        
        IF (record_count > 0) THEN
            CONTINUE;
        END IF;
        
        tablename := UPPER(folder_name);
        full_stage_path := '@walmart_raw_stage/' || file_path;
        
        -- Check if table exists
        SELECT COUNT(1) INTO :record_count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
            AND TABLE_NAME = :tablename;
            
        -- Create table if it doesn't exist
        IF (record_count = 0) THEN
            create_sql := '
                CREATE OR REPLACE TABLE IDENTIFIER (?)
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => ?,
                            FILE_FORMAT => ''my_csv_format'',
                            IGNORE_CASE => TRUE
                        )
                    )
                )';
            EXECUTE IMMEDIATE :create_sql USING (tablename, full_stage_path);
        END IF;
        
        -- Load the data from the file into the target table
        copy_sql := '
            COPY INTO IDENTIFIER(?)
            FROM ?
            FILE_FORMAT = (FORMAT_NAME = ''my_csv_format'')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ''SKIP_FILE''';
        
        EXECUTE IMMEDIATE :copy_sql USING (tablename, full_stage_path);
        
        -- Log the processed file
        INSERT INTO FILE_LOAD_LOG (FILE_NAME) VALUES (:file_path);
        
    END FOR;
    
    RETURN 'Dynamic ingestion from S3 to snowflake complete.';
END;
$$;


CALL DYNAMIC_TABLE_LOAD();

select * from fact;

SELECT * FROM FILE_LOAD_LOG ORDER BY LOAD_TIME DESC;

SHOW TABLES;


-- Final setup
CREATE OR REPLACE TASK walmart_auto_ingest_task
    WAREHOUSE = WALMART_WH
    SCHEDULE = 'USING CRON */5 * * * * UTC'
    COMMENT = 'Automatically loads files from S3'
AS
    CALL DYNAMIC_TABLE_LOAD();

drop task walmart_auto_ingest_task;

ALTER TASK walmart_auto_ingest_task RESUME;


-- Verify it's running
SHOW TASKS;




select * from directory(@fact_raw_stage);

list @fact_raw_stage;

create or replace pipe wal_fact_pipe
auto_ingest = true AS
copy into FACT
from @fact_raw_stage
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

create or replace pipe walmart_s3_pipe
auto_ingest = true AS
copy into department
from @walmart_raw_stage
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

SELECT SYSTEM$PIPE_STATUS('wal_fact_pipe');

SHOW PIPES;

select * from fact;


drop schema walmart_db.information_schema;

SELECT * FROM raw_data_analytics.walmart_date_dim LIMIT 10;

select * from bronze_analytics.walmart_date_dim;
select * from bronze_analytics.walmart_store_dim;
select * from bronze_analytics.walmart_fact_table;