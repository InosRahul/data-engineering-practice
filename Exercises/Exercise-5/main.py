import psycopg2
import glob
import os

create_table_accounts = '''
    DROP TABLE IF EXISTS accounts CASCADE;
    CREATE TABLE accounts (
        customer_id INTEGER PRIMARY KEY NOT NULL,
        first_name TEXT,
        last_name TEXT,
        address_1 TEXT,
        address_2 TEXT,
        city TEXT,
        state TEXT,
        zip_code INTEGER,
        join_date DATE
    );
'''

create_table_products = '''
    DROP TABLE IF EXISTS products CASCADE;  
    CREATE TABLE products (
        product_id INTEGER PRIMARY KEY NOT NULL,
        product_code INTEGER,
        product_description TEXT
    );
'''

create_table_transactions = '''
    DROP TABLE IF EXISTS transactions CASCADE;
    CREATE TABLE transactions (
       transaction_id TEXT PRIMARY KEY NOT NULL,
       transaction_date DATE,
       product_id INTEGER,
       product_code INTEGER,
       product_description TEXT,
       quantity INTEGER,
       account_id INTEGER,
       FOREIGN KEY(product_id) REFERENCES products(product_id),
       FOREIGN KEY(account_id) REFERENCES accounts(customer_id)
    );
'''

desc_accounts = '''
    select column_name, data_type, character_maximum_length, column_default, is_nullable
    from INFORMATION_SCHEMA.COLUMNS where table_name = 'accounts';
'''

desc_products = '''
    select column_name, data_type, character_maximum_length, column_default, is_nullable
from INFORMATION_SCHEMA.COLUMNS where table_name = 'products';
'''

desc_transactions = '''
    select column_name, data_type, character_maximum_length, column_default, is_nullable
from INFORMATION_SCHEMA.COLUMNS where table_name = 'transactions';
'''

select_all_accounts = '''
    SELECT * from accounts
'''

select_all_products = '''
    SELECT * from products
'''

select_all_transactions = '''
    SELECT * from transactions
'''


def ingestCSVToPostgres(filepath:str, cursor, conn):
    filename = filepath.split("/")[-1].replace(".csv", "")
    with open(filepath) as f:
        next(f)
        cursor.copy_from(file=f, table=filename, sep=',')
        conn.commit()



def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    cursor = conn.cursor()

    cursor.execute(create_table_accounts)
    cursor.execute(create_table_products)
    cursor.execute(create_table_transactions)

    files = glob.glob(os.getcwd() + "/data/*.csv", recursive=True)
    for file in files:
        ingestCSVToPostgres(file, cursor, conn)

    
    cursor.execute(select_all_accounts)
    rows_accounts = cursor.fetchall()
    for a in rows_accounts:
        print(a)

    cursor.execute(select_all_products)
    rows_products = cursor.fetchall()
    for p in rows_products:
        print(p)  

    cursor.execute(select_all_transactions)
    rows_transactions = cursor.fetchall()
    for t in rows_transactions:
        print(t)
    
    conn.close()


if __name__ == "__main__":
    main()
