
from dbConnectionPool import ConnectionPool
import time
import concurrent.futures
from psycopg2.errors import UniqueViolation
import psycopg2

x=ConnectionPool(database='test_mailbox')
workes=40
start_time=time.time()
#x.conn_idle_timeout=20

def update_db(curr,username,password,seconds=0):
        try:
                curr.execute("UPDATE users SET password_hash = %s WHERE username = %s;", (password, username))
                time.sleep(seconds) 
                return("Update done !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        except UniqueViolation:
            return (False, "User already exists")
        except psycopg2.Error as e:
            print("Database error:", e)
            return (False, "User cannot be created")

def insert_db(curr,username='user',password='###',seconds=0):
        try:
                curr.execute("INSERT INTO users (username,password_hash) VALUES (%s, %s);", (username, password))
     #           curr.execute("SELECT username,password_hash FROM users WHERE username = %s;", (username,))
                time.sleep(seconds) 
                return("Insert done $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        except:
               return "Inster cannot be done $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"

def test_function_update(nuser,npassword,seconds):
        try:
                conn0=x.get_connection()
                curr0 = conn0.cursor() #set cursor
                # execute querry
                dummy_result = update_db(curr0,nuser,npassword)
                # wait and then release connection  
                conn0.conn.commit()   
                time.sleep(seconds)    
                x.release_connection(conn0)
                return dummy_result         
        except Exception as e:
                return ("Error:", e)

def test_function_instert(nuser,npassword,seconds):
        try:
                conn1=x.get_connection()
                curr1 = conn1.cursor() #set cursor
                # execute querry
                dummy_result=insert_db(curr1,nuser,npassword)
                conn1.conn.commit()  
                # wait and then release connection        
                time.sleep(seconds)  
                x.release_connection(conn1)
                return dummy_result
        except Exception as e:
             return ("Error:", e)

def test_function_select(seconds):
        try:
            conn=x.get_connection()
            curr = conn.cursor() #set cursor
            # execute querry
            curr.execute("SELECT username,password_hash FROM users;")
            # wait and then release connection        
            time.sleep(seconds) 
            conn.conn.commit()  
            x.release_connection(conn)
            return (curr.fetchall())
        except Exception as e:
             return ("Error:", e)

seconds=[0.5 * i for i in range(workes*2)]
new_user=[("user"+str(n),"pass"+str(n)) for n in range(40)]
zusernames,zpasswords = zip(*new_user)       


# with concurrent.futures.ThreadPoolExecutor(max_workers=workes*3) as executor:
#         results=executor.map(test_function_select,seconds)
#         results1=executor.map(test_function_instert,zusernames,zpasswords,seconds)
#         results2=executor.map(test_function_update,zusernames,zpasswords,seconds)

#         for f in results:
#                 print(f"First print:{f} Connection number: {len(x.connections)}")
#         for f1 in results1:
#                 print(f"Second print:{f1} Connection number: {len(x.connections)}")
#         for f2 in results2:
#                 print(f"Third print:{f2} Connection number: {len(x.connections)}")


with concurrent.futures.ThreadPoolExecutor(max_workers=workes*3) as executor:
    futures = [
        executor.submit(test_function_select, sec) for sec in seconds
    ] + [
        executor.submit(test_function_instert, u, p, s)
        for u, p, s in zip(zusernames, zpasswords, seconds)
    ] + [
        executor.submit(test_function_update, u, p, s)
        for u, p, s in zip(zusernames, zpasswords, seconds)
    ]

    for future in concurrent.futures.as_completed(futures):
        try:
            result = future.result()
            print(f"Result: {result} number of connetions {len(x.connections)}")
        except Exception as e:
            print(f"Future failed with: {e}")


end_time=time.time()
time.sleep(5)
print(f"End of the test, number of connetions {len(x.connections)}")
duration=end_time-start_time
print(f"{duration:.3f}")