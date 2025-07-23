import sqlite3
from dbConnectionPool import ConnectionPool
from dbConnectionPool import PooledConnection
import hashlib


cp=ConnectionPool(database='mailbox')


try:
    conn=cp.get_connection()

    curr=conn.cursor()
    task1="""INSERT INTO users(username, password_hash,is_admin) VALUES(?,?,?)"""
    login="admin"
    password='admin'
    password=str(hashlib.sha256(password.encode()).hexdigest())
    values=(login,password,True)
    print(values)
    curr.execute(task1,values)
    conn.commit()

except sqlite3.OperationalError as e:
    print("Failed to create tables:", e)

con1=cp.get_connection()
cur1=con1.cursor()
cur1.execute("SELECT * FROM users")
print(cur1.fetchall())
