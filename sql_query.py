# create table account

create_table_account = """create table if not exists account (
account_id	 float,
limit_ 	float,
username 	varchar,
name_ 	varchar,
products varchar
);"""


# create table customer 
create_table_customer = """create table if not exists customer (
	username		varchar,
	name_			varchar,
	address			varchar,
	birthdate	timestamp,
	email		varchar,
	active		boolean

);"""


# create table transaction_summary

create_table_trans_dtl = """create table if not exists transaction_detail (
	account_id			float,
	date_of_trans		timestamp,
	amount				float,
	transaction_code	varchar,
	symbol				varchar,
	price				varchar,
	total_amount		varchar
);"""

# create table transaction_detail

create_trans_smtr = """create table if not exists  transaction_summary (
	account_id			float,
	transaction_count	float,
	bucket_start_date	timestamp,
	bucket_end_date		timestamp
);"""




