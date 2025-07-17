import psycopg2
from psycopg2.errors import UniqueViolation
from datetime import datetime
from contextlib import contextmanager
import hashlib
from my_classes.dbConnectionPool import ConnectionPool

class DataBaseService():
    def __init__(self, host='localhost', database='mailbox', user='postgres', password='admin'):
        self.host = host
        self.database = database
        self.user = user
        self.password = password     
        self.CP=ConnectionPool(self.database)
        self._ensure_tables()

    @contextmanager #to work with with
    def _get_cursor(self): 
        conn = self.CP.get_connection()
        try:
            curr = conn.cursor() #set cursor
            yield curr #give curr as a output
            conn.conn.commit() 
        except Exception as e:
            conn.conn.rollback()
            raise e
        finally: #after with code close connection.
            curr.close()
            conn.conn.close()  

    def _hash_password(self,password):
        return hashlib.sha256(password.encode()).hexdigest()

    def _ensure_tables(self):
        with self._get_cursor() as curr:
            curr.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_admin BOOL DEFAULT FALSE
                );
            """)

            curr.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    receiver_id INTEGER REFERENCES users(id),
                    sender_id INTEGER REFERENCES users(id),
                    message VARCHAR(255) NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)

    def _format_messages(self, id: int, sender: str, content: str, time: datetime):
        return f"""Message number:{id}\nfrom: {sender}\ntime: {time}\n{content}\n"""

    def create_user_check(self, username: str):
        with self._get_cursor() as curr:
            curr.execute("SELECT id FROM users WHERE username = ?;", (username,))
            return curr.fetchone() is None # if fetchone false then return true

    def create_user(self, username: str, password: str):
        try:
            with self._get_cursor() as curr:
                curr.execute("INSERT INTO users (username, password_hash) VALUES (?, ?);", (username, password))
            return (True, "User created")
        except UniqueViolation:
            return (False, "User already exists")
        except psycopg2.Error as e:
            print("Database error:", e)
            return (False, "User cannot be created")

    def check_user(self, username: str):
        """
        Returns:
            Optional[Tuple[int, str, str]]: A tuple containing (id, username, password_hash) if the user exists,
            otherwise None.
        """
        with self._get_cursor() as curr:
            curr.execute("SELECT id, is_admin, password_hash FROM users WHERE username = ?;", (username,))
            return curr.fetchone()

    def password_change(self, id_user: int, password: str):
        """
        Returns:
            Optional Tuple(True, "Password updated successfully") if the password has been changed,
            otherwise Tuple (False, "Could not update password")
        """
        try:
            with self._get_cursor() as curr:
                curr.execute("UPDATE users SET password_hash = ? WHERE id = ?;", (password, id_user))
            return (True, "Password updated successfully")
        except psycopg2.Error as e:
            print("Database error:", e)
            return (False, "Could not update password")

    def get_id_by_user(self, username: str):
        """
        Returns: int or False if user not found
        """
        with self._get_cursor() as curr:
            curr.execute("SELECT id FROM users WHERE username = ?;", (username,))
            result = curr.fetchone()
            return result[0] if result else False

    def get_user_by_id(self, id: int):
        with self._get_cursor() as curr:
            curr.execute("SELECT username FROM users WHERE id = ?;", (id,))
            result = curr.fetchone()
            return result[0] if result else None

    def admin_all_users(self):
        with self._get_cursor() as curr:
            curr.execute("SELECT username FROM users;")
            return curr.fetchall() or None

    def msg_count(self, id_user: int):
        with self._get_cursor() as curr:
            curr.execute("SELECT COUNT(*) FROM messages WHERE receiver_id = ?;", (id_user,))
            return curr.fetchone()[0] #first argument of fuccntion fetchone

    def load_message(self, id_receiver: int):
        """
        Input receiver_id
        Returns: list with messages for defined user if not found None
        """
        with self._get_cursor() as curr:
            curr.execute("""
                SELECT 
                    messages.id,
                    us.username AS sender_name,
                    ur.username AS receiver_name,
                    messages.message,
                    messages.timestamp
                FROM messages
                JOIN users AS us ON messages.sender_id = us.id
                JOIN users AS ur ON messages.receiver_id = ur.id
                WHERE ur.id = ?;
            """, (id_receiver,))
            data = curr.fetchall()
            result = []
            for id, sender, receiver, content, time in data:
                formatted_time = time.strftime("%Y-%m-%d %H:%M:%S")
                message = self._format_messages(id, sender, content, formatted_time)
                result.append(message)
            return result

    def write_message(self, receiver: int, sender: int, content: str):
        """
        Inputs: receiver id,sender id ,content of message
        Returns:
           True if message is write in database,
            otherwise False
        """
        try:
            with self._get_cursor() as curr:
                curr.execute("INSERT INTO messages (receiver_id, sender_id, message) VALUES (?, ?, ?);", (receiver, sender, content))
            return True
        except:
            return False

    def delete_all_message(self, user: int):
        """
        Inputs: user id,
        Returns:
            Delete all messages for definied user
        """
        try:
            with self._get_cursor() as curr:
                curr.execute("DELETE FROM messages WHERE receiver_id = ?;", (user,))
            return True
        except:
            return False

    def delete_one_message(self, id: int):
        """
        Inputs: id_message
        Returns:
            Delete the message with the given id 
        """
        try:
            with self._get_cursor() as curr:
                curr.execute("DELETE FROM messages WHERE id = ?;", (id,))
            return True
        except:
            return False
