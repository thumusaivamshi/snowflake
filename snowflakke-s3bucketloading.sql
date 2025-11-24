create storage integration s3_first
type=external_stage
storage_provider=s3
enabled=true
storage_aws_role_arn='arn:aws:iam::180294183932:role/snowflakeuser1'
storage_allowed_locations=('s3://snowflakebucket7075/csv/');

desc storage integration s3_first;

create or replace table db1.external_stages.table3(
index int,customer_id string,first_name varchar,last_name varchar,company varchar,city string,country string,phone1 string,phone2 string,email string,subscriptiodate date,website string);


CREATE OR REPLACE FILE FORMAT db1.external_stages.csvfileformat
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_DELIMITER = ','
EMPTY_FIELD_AS_NULL = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE;

create or replace stage db1.external_stages.s3stage
url='s3://snowflakebucket7075/csv/'
storage_integration=s3_first
file_format=db1.external_stages.csvfileformat;

list @db1.external_stages.s3stage;

copy into db1.external_stages.table3
from @db1.external_stages.s3stage
file_format=db1.external_stages.csvfileformat;

select * from db1.external_stages.table3;