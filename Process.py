from pyspark.sql import SparkSession
from parameter import *
from pyspark.sql.functions import col


# create sparksession:
connect_mongo = 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.jars.packages",connect_mongo ) \
        .config("spark.jars", "D:\Spark\jdbc\ojdbc8.jar,D:\Spark\jdbc\postgresql-42.6.0.jar") \
        .getOrCreate()

print("already create sparksession, ready to process customer collection")

def process_customer(uri, database, collection,pipeline):
    # read collection "customer" with pipeline --> create 'customer' dataframe
    customer = spark.read.format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .option("pipeline", pipeline) \
            .load()
    # create clean_customer: just select some fields from customer dataframe: 
    clean_customer = customer.select(col("username"), col("name"), col("address"), col("birthdate"), col("email"), col("active"))

    #create customer_acc: unwid field "accounts" and just select some field: 
    customer_acc = customer.select(col("accounts"), col("username"), col("name"))

    print("already process customer collection, return: clean_customer and customer_acc ")
    clean_customer = clean_customer.withColumnRenamed("name", "name_")
    return clean_customer, customer_acc


def process_account(uri, database, collection, customer_acc):
    # read account collection:
    account = spark.read.format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .load()
    
    # make clean account datarame: join vs customer_acc dataframe:
        # make temp table for join: 
    account.createOrReplaceTempView("account")
    customer_acc.createOrReplaceTempView("customer_acc")
        # join: 
    clean_account = spark.sql("""
                            select t1.account_id,
                                    t1.limit,
                                    t2.username,
                                    t2.name,
                                    t1.products

                            from account t1
                            left join customer_acc t2
                            on t1.account_id = t2.accounts

                            """)
    
    print("already process account collection, ready for next step")
    clean_account = clean_account.withColumnRenamed("limit", "limit_")\
                                 .withColumnRenamed("name", "name_")
    return clean_account

def process_transaction(uri, database, collection, pipeline):
    # read transaction collection:
    transaction = spark.read.format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .option("pipeline", pipeline) \
            .load()
    # create clean_transaction_summary: include summary information about account's transaction
    clean_trans_smr = transaction.select(col("account_id"), col("transaction_count"),col("bucket_start_date"),col("bucket_end_date"))

    #create clean_transaction_detail: include summary information for each transaction:
    clean_trans_dtl = transaction.select(col("account_id"), col("date_of_trans"), col("amount"), col("transaction_code"), col("symbol"), col("price"), col("total_amount"))
    print("alredy process transaction collection")
    return clean_trans_dtl, clean_trans_smr