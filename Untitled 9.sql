create storage integration s3_bucket
type=external_stage
storage_provider=s3
enabled=true
storage_aws_role_arn='arn:aws:iam::180294183932:role/snowflakeuser1'
storage_allowed_locations=('s3://snowflakebucket7075/csv/');

desc storage integration s3_bucket;

create or replace table db1.external_stages.table4(
index string,organization_id string,name string,website string,country string,description string,founded int,industry string,number_of_employees int);

create or replace file format db1.external_stages.csvformat2
type=csv
skip_header=1
field_delimiter=','
EMPTY_FIELD_AS_NULL = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE;

create or replace stage db1.external_stages.s3stage1
url='s3://snowflakebucket7075/csv/organizations-100.csv'
storage_integration=s3_bucket
file_format=db1.external_stages.csvformat2;

list @db1.external_stages.s3stage1;

copy into db1.external_stages.table4
from @db1.external_stages.s3stage1;

select * from db1.external_stages.table4;


--on_error='continue';
--on_error='skip_file_error-limit(innumubers)';

--validation_mode=return_errors, return_n_errors;

--size_limit=num;

--return_failed_only=true\false   only shows that file that have errors

--truncatecolumns=true|false;  manages the columns have a maximum string size

--force=true\false it will be loaded again it leads to potential duplicatun of table