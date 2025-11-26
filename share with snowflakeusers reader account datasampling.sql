create or replace database dat_S;

create or replace stage aws_share_stage
url='s3://bucketsnowflakes3/';

list @aws_share_stage;

create or replace table orders(order_id varchar,
amount int,profit int,quantity int,category varchar,subcategory varchar);

copy into orders
from @aws_share_stage
file_format=(type=csv,field_delimiter=',',skip_header=1)
pattern='.*OrderDetails.*';


create transient table orders_transient
clone orders;

select * from orders_transient;

create or replace table orderswaptable(order_id varchar,
amount int,profit int,quantity int,category varchar,subcategory varchar);

alter table orderswaptable
swap with orders_transient;

select * from orderswaptable;

show tables;

create or replace share share1;   --- creates share which is nothing but a container 

grant usage on database dat_s to share share1;
grant usage on schema dat_s.public to share share1;  -- granting permissions to share for databse and schema

grant select on table dat_s.public.orders to share share1; -- we use select statement for sharing table


GRANT SELECT ON ALL TABLES IN SCHEMA DAT_S.PUBLIC TO SHARE2;
GRANT SELECT ON ALL TABLES IN DATABASE DAT_S TO SHARE2;
show grants to share share1;   --shows the grants give to this share

alter share share1 add account=<producer-name>; -- wrong   we are producer and we sharing to consumer so we need to give information to consumer account.
ALTER SHARE share1 ADD ACCOUNTS = AA32774;  --- right


show shares;

create database database_share from share <producerid>.dataabse_name;

select * from database_share;  --- we can only share one database.


CREATE OR REPLACE SHARE SHARE2;

CREATE MANAGED ACCOUNT READER2
ADMIN_NAME=READER_2
ADMIN_PASSWORD='Reader2@1234567890'
TYPE=READER;

show managed accounts;

grant usage on database dat_s to share share2;
grant usage on schema dat_s.public to share share2;
GRANT SELECT ON ALL TABLES IN SCHEMA DAT_S.PUBLIC TO share share2;
GRANT SELECT ON ALL TABLES IN DATABASE DAT_S TO share share2;

alter share share2 add account=RY06703;


select * from dat_s.public.orders;

update dat_s.public.orders
set profit=100;

select * from dat_s.public.orders; -- we cant do changes n reader account but if we change in main account then it changes in all reader and user accpoit



create or replace view dat_s.public.customerview as 
select order_id,amount,category from dat_s.public.orders
where amount>=250;

show views;

grant select on view dat_s.public.customerview to role public;  -- non secure view


create or replace secure view dat_s.public.customersecureview as 
select order_id,amount,category from dat_s.public.orders
where amount>=250;

show views;

create or replace share view_share;

grant usage on database dat_s to share view_share;
grant usage on schema dat_s.public to share view_share;

grant select on dat_s.public.customerview to share view_share;
grant select on dat_s.public.customersecureview to share view_share;

alter share view_share
set secure_objects_only=true;

show views;


alter share view_share add account=RY06703;

grant reference_usage on database employees to share view_share; --- when we want add columns from different databases;


create managed account vamshi_reader
admin_name=vamshi_reader_admin
admin_password='Vamshi@123456789'
type='reader';

show accounts;

show managed accounts;

SHOW SHARES;


--data sampling
--why sampling we have two types of smpling bernouli sampling and system sampling;

--row based or bernouli method     every row is chosen with percentage p  
--it is considered as random
--smaller tables

--block or system method  we use block is chosen with percentage p   we use micro partitions;
--more efficient processing
--larger tables




--sample row(1) seed(27)   -- row based


--sample system(1) seed(23);



select * from snowflake_sample_data.tpcds_sf10tcl.customer;   -- it took 1m 1second


select *from snowflake_sample_data.tpcds_sf10tcl.customer
sample row(1) seed(27);  -- it took 2.3 sec


select *from snowflake_sample_data.tpcds_sf10tcl.customer
sample system(1) seed(27); -- it took 2.2 sec 