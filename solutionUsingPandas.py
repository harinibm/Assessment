import pandas as pd
import os
import sqlalchemy

sqliteConnection = sqlalchemy.create_engine('sqlite:///' + os.getcwd() + '/s30ETLAssignment.db')

Sales = pd.read_sql('select * from Sales', sqliteConnection)
Customers = pd.read_sql('select * from Customers', sqliteConnection)
Orders = pd.read_sql('select * from Orders', sqliteConnection)
Items = pd.read_sql('select * from Items', sqliteConnection)

Result = pd.merge(Customers, Sales, on='customer_id')
Result = pd.merge(Result, Orders, on='sales_id')
Result = pd.merge(Result, Items, on='item_id')

Result = Result.drop(columns=['sales_id', 'order_id', 'item_id'])

Result = Result.groupby(['customer_id', 'age', 'item_name'], as_index=False)["quantity"].sum()

Result = Result.loc[(Result['age'] >= 18) & (Result['age'] <= 35) & (Result['quantity'] != 0.0)]

Result['quantity'] = Result['quantity'].astype(int)

Result.rename(
    columns={
        'customer_id': 'Customer',
        'age': 'Age',
        'item_name': 'Item',
        'quantity': 'Quantity'
    },
    inplace=True
)

Result.to_csv('resultUsingPandas.csv', index=False, sep=';')

dirpath = os.getcwd() + "/resultUsingPandas.csv"
print("Result exported Successfully into {}".format(dirpath))
