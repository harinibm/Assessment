import sqlite3
import os
import csv

try:
    sqliteConnection = sqlite3.connect('s30ETLAssignment.db')
    cursor = sqliteConnection.cursor()
    print("Database successfully Connected to SQLite")

    query = """
    select c.customer_id as Customer, c.age as Age, i.item_name as Item, sum(o.quantity) as Quantity
    from Customers c
    left join Sales s
    on s.customer_id = c.customer_id
    left join Orders o
    on o.sales_id = s.sales_id
    left join Items i
    on i.item_id = o.item_id
    where o.quantity is not null
    group by c.customer_id, o.item_id
    having c.age >=18 and c.age <=35 
    """

    cursor.execute(query)

    with open("resultUsingSQL.csv", "w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=";")
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)

    dirpath = os.getcwd() + "/resultUsingSQL.csv"
    print("Result exported Successfully into {}".format(dirpath))

    cursor.close()

except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed")
