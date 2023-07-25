# Declare MonogDB variable:
mongo_host = "localhost"
mongo_port = 27017
uri = f"mongodb://{mongo_host}:{mongo_port}"
database = 'analyst'
collection_account = 'account'
collection_customer = 'customer'
collection_transaction = 'transaction'


# Declare PostgreSQL variable:
dbname = "transaction"
user = "postgres"
password = "postgres"
server = "localhost"
port = "5432"
url = f"jdbc:postgresql://{server}:{port}/{dbname}"
driver = "org.postgresql.Driver"
account_table = "account"
customer_table = 'customer'
trans_dtl_table = 'transaction_detail'
trans_smr_table = 'transaction_summary'


# Declare pipeline:
pipeline_cus = [
                { 
                    "$unwind" : "$accounts"       
                }
               ]

pipeline_trans = [
     
                {'$unwind': '$transactions'},
                {
                    '$addFields': {
                    'date_of_trans': '$transactions.date',
                    'amount': '$transactions.amount' ,
                    'transaction_code': '$transactions.transaction_code',
                    'total_amount' : '$transactions.total',
                    'symbol': '$transactions.symbol',
                    'price' :'$transactions.price',
                    'total_amount': '#transactions.total'

                    }
                }
                ]