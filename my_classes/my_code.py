import sqlite3
from dbConnectionPool import ConnectionPool
from dbConnectionPool import PooledConnection


cp=ConnectionPool()
project = ('Cool App with SQLite & Python', '2015-01-01', '2015-01-30')

def add_project(conn:PooledConnection, project):
    # insert table statement
    sql = ''' INSERT INTO projects(name,begin_date,end_date)
              VALUES(?,?,?) '''
    
    # Create  a cursor
    cur = conn.cursor()

    # execute the INSERT statement
    cur.execute(sql, project)
    
    # commit the changes
    conn.commit()

    # get the id of the last inserted row
    return cur.lastrowid

try:
    conn=cp.get_connection()

    curr=conn.cursor()
    task1="""CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY, 
        name text NOT NULL, 
        begin_date DATE, 
        end_date DATE
    );"""
    curr.execute(task1)
    conn.commit()
   # project_id = add_project(conn, project)
  #  print(f'Created a project with the id {project_id}')

except sqlite3.OperationalError as e:
    print("Failed to create tables:", e)

con1=cp.get_connection()
cur1=con1.cursor()
sql_delete = 'DELETE FROM projects WHERE id = ?'
cur1.execute(sql_delete, (2,))
cur1.execute(sql_delete, (3,))
cur1.execute("SELECT * FROM  projects")
print(cur1.fetchall())
