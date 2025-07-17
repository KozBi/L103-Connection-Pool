import threading
import sqlite3
import time


class PooledConnection:
    def __init__(self,database="database_file.db"):
        self.database = database
        self.conn=self._create_connection()
        self.in_use=False
        self.last_used_time=None

    def _create_connection(self):
        """
        Returns:
            Object type connection
        """
        try:
            return sqlite3.connect(self.database)
        except sqlite3.OperationalError as e:
            print("Failed to open database:", e)
    
    def cursor(self):
        return self.conn.cursor()
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        self.conn.close()
class ConnectionPool:
    """
    Custom connection pool for PostgreSQL.

    Args:
        database (str, optional): SQLite database filename. Defaults to "database_file.db".


    Attributes:
        conn_idle_timeout (int): Timeout (in seconds) after which connections are closed.
        min_connection (int): Minimum number of connections to keep in pool.
        max_connection (int): Maximum number of connections allowed in pool.
    """
    def __init__(self,database="database_file.db"):
        self.min_connection=5
        self.max_connection=99
        self.conn_idle_timeout=2 # Max idle time in seconds before closing connections

        self.database = database

        self.lock = threading.Lock()
        self.connections= [PooledConnection(self.database) for _ in range(self.min_connection)]
        #deamon true Thread can be close WHENEVER
        self.connections_check = threading.Thread(target=self._check_connections,daemon=True)
        self.connections_check.start()


    def get_connection(self,wait_timeout=30, poll_interval=0.1):
        start_time = time.time()
        while True:
            with self.lock: 
                for conn in self.connections:
                    if not conn.in_use:
                        conn.in_use=True
                        return conn
                
                if len(self.connections)<self.max_connection:
                    new_con=self._create_new_connection()
                    new_con.in_use=True
                    return new_con
            #build in exception used when no connection left

            # If no free connections wait:
            if time.time() - start_time >= wait_timeout:
                raise ConnectionError("Timeout: No connection available after waiting.")
            time.sleep(poll_interval)
        

    def release_connection(self,connection:PooledConnection):
        with self.lock: 
            STATUS_INTRANS = 2
            if connection in self.connections:
                # if connection.conn.status == STATUS_INTRANS:
                #     print("⚠️ Warning: Connection released with open transaction! Autocommit !.")
                #     connection.conn.commit()

                connection.in_use=False
                connection.last_used_time=time.time()
                return
            else:
                raise ValueError("This connection does not belong to the Pool!")
    
    def _create_new_connection(self):
        new_conn=PooledConnection(self.database)
        self.connections.append(new_conn)
        return new_conn
    
    def _check_connections(self):
        #this thread works in a endless loop
        while True:
            with self.lock: 
                #close unsed connection after idle itme min
                current_time=time.time()
                l_deleted_conn=[]
                #keep minimal connections
                while len(self.connections)<self.min_connection:
                    self._create_new_connection()

                for conn in (self.connections[5:]):
                    
                    # #delete broken connection 
                    # if conn.conn.closed !=0 and conn.in_use:
                    #     l_deleted_conn.append(conn)
                    #     continue # interrupt current iteration

                    #check not used connection and remove when they are not used, but keep minial connection.
                    if not conn.in_use:
                        if conn.last_used_time is None or current_time-conn.last_used_time > self.conn_idle_timeout:
                            conn.conn.close()
                            l_deleted_conn.append(conn)
                
                #list cannot be modified in the loop, so logic below
                for deleted_conn in l_deleted_conn:
                    self.connections.remove(deleted_conn)
            time.sleep(self.conn_idle_timeout)


    