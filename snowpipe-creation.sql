create or replace database snowflake1;

create or replace schema snowflake1.snowpipes;

create or replace schema snowflake1.file_formats;

create or replace schema snowflake1.external_stages;

create or replace schema tables;

create or replace file format snowflake1.file_formats.csv_format
type=CSV
field_delimiter=','
skip_header=1;

desc file format snowflake1.file_formats.csv_format;

create or replace table snowflake1.tables.employees(
id int,first_name string,last_name string,email string,location string,department string
);


create storage integration s3_workspace
type=external_stage
storage_provider=s3
enabled=true
storage_aws_role_arn='arn:aws:iam::180294183932:role/snowpipe'
storage_allowed_locations=('s3://employeeinfo7075/');

desc storage integration s3_workspace;


-- once you create storage integration go to roles and edit trust policy to establish a connection between aws and snow flake.

create or replace stage snowflake1.external_stages.stage1
url='s3://employeeinfo7075/'
storage_integration=s3_workspace
file_format=snowflake1.file_formats.csv_format;

list @snowflake1.external_stages.stage1;

create or replace pipe snowflake1.snowpipes.employeepipe
auto_ingest=true
as
copy into snowflake1.tables.employees
from @snowflake1.external_stages.stage1;

desc pipe snowflake1.snowpipes.employeepipe;

--once pipe is created search for notification_channel and then ope the storage s3 bucket and create an even notification to trigger the snowpipe.

select count(*) from snowflake1.tables.employees;

-- wait for some time to load and to know the status of pipe we can use 

select system$pipe_status('snowflake1.snowpipes.employeepipe');

select * from table(information_schema.copy_history(table_name=>'snowflake1.tables.employees',start_time=>dateadd(hour,-2,current_timestamp())));