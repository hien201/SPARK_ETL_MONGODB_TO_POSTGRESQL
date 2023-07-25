from parameter import* 
from Process import * 


def load_account(clean_account, account_table):
    clean_account.write.format('jdbc') \
            .option("url", url) \
            .mode("append") \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", account_table) \
            .option("driver", driver) \
            .save()
    print("already load data to account table")

def load_customer(clean_customer, customer_table):
    clean_customer.write.format('jdbc') \
            .option("url", url) \
            .mode("append") \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", customer_table) \
            .option("driver", driver) \
            .save()
    print("already load data to customer table")

def load_transaction(clean_trans_smr, clean_trans_dtl, trans_smr_table, trans_dtl_table):
    clean_trans_smr.write.format('jdbc') \
            .option("url", url) \
            .mode("append") \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", trans_smr_table) \
            .option("driver", driver) \
            .save()
    print("already load data to trans_smr table")

    clean_trans_dtl.write.format('jdbc') \
            .option("url", url) \
            .mode("append") \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", trans_dtl_table) \
            .option("driver", driver) \
            .save()
    print("already load data to trans_dtl table")