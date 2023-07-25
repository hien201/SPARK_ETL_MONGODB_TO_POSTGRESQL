from parameter import *
from Process import *
from load_to_postgresql import * 


def main():
    #process collection: 
    clean_customer, customer_acc = process_customer(uri, database, collection_customer ,pipeline_cus)
    clean_account = process_account(uri, database, collection_account , customer_acc)
    clean_trans_dtl, clean_trans_smr = process_transaction(uri, database, collection_transaction , pipeline_trans)

    #load to postges
    load_account(clean_account, account_table)
    load_customer(clean_customer, customer_table)
    load_transaction(clean_trans_smr, clean_trans_dtl, trans_smr_table, trans_dtl_table)

if __name__ == '__main__':
    main()
    print("HIEN! FINISH ETL JOB, LET'S GO TO SLEEP")