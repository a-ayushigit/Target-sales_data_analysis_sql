import pandas as pd
import mysql.connector as msq
import os

csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('order_items.csv', 'order_items'),
    ('payments.csv', 'payments'),
    ('geolocation.csv', 'geolocation')
]

conn = msq.connect(
    host='localhost',
    user='root',
    password='AnyuSQL21@25',
    database='sales_data'
)

cursor = conn.cursor()

folder_path = r'C:\Users\prakr\Downloads\sales_data'


def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'


def load_sql():
    for csv_file, table_name in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        df = pd.read_csv(file_path)
        df = df.where(pd.notnull(df), None)
        print(f"Processing {csv_file}")
        print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

        df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

        columns = ', '.join([f'`{col}`{get_sql_type(df[col].dtype)}' for col in df.columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
        cursor.execute(create_table_query)

        for _, row in df.iterrows():
            values = tuple(None if pd.isna(x) else x for x in row)
            sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
            cursor.execute(sql, values)

        conn.commit()
        print(f"CSV file {csv_file} processed .... Code executed with no error :)")

    conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_sql()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
