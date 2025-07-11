import unittest
import psycopg2
import json
from my_classes.UserCommandHandler import UserCommandHandler
from my_classes.UserMenager import UserMenager
from my_classes.DataBaseService import DataBaseService

class TestUserMenagerIntegration(unittest.TestCase):

    #classmethod,  to run only once connection to temp database
    @classmethod
    def setUpClass(cls):
        cls.conn=psycopg2.connect(
                host='localhost',
                database='test_mailbox',       
                user='postgres',      
                password='admin')
        cls.curs=cls.conn.cursor()

    def setUp(self):
        self.database=DataBaseService(database="test_mailbox")
        self.reset_database()
        self.UCH=UserCommandHandler(self.database)
       
    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    #reset test_mailbox each tim when test is called.
    def reset_database(cls):
        #reset whole table
        cls.curs.execute("TRUNCATE TABLE users RESTART IDENTITY CASCADE;")
        passwords=["admin","bob","adam3"]
        with open("tests/fixtures/test_Users.json", "r", encoding="utf-8" ) as f:
            users = json.load(f)
            # when you work with list type, zip is better than enumerate
        for u, p in zip(users,passwords):
            hashed = UserMenager._hash_password(p)
            if u.get("is_admin"):
                is_adm=u.get("is_admin")
            else: is_adm=None
            cls.curs.execute(""" INSERT INTO users (username, password_hash, is_admin) VALUES (%s,%s,%s);""", 
                            (u["username"],hashed, is_adm)) 

        ### messages
        cls.curs.execute("TRUNCATE TABLE messages RESTART IDENTITY CASCADE;")
        with open("tests/fixtures/test_Messages.json", "r", encoding="utf-8" ) as f:
            messages = json.load(f)
            for m in messages:
                receiver_id=m.get("receiver")
                sender_id=m.get("sender")
                content=m.get("content")
                cls.curs.execute(""" INSERT INTO messages (receiver_id, sender_id, message) VALUES (%s,%s,%s);""", 
                            (receiver_id,sender_id, content))
                
        cls.conn.commit()


    
    def test_get_user_id(self):
        
        result1=self.UCH.UserMenager.get_user_by_id(1)
        self.assertEqual(result1,"admin")

        result2=self.UCH.UserMenager.get_user_by_id(2)
        self.assertEqual(result2,"bob")

        result3=self.UCH.UserMenager.get_id_by_user("admin")
        self.assertEqual(result3,1)

        result4=self.UCH.UserMenager.get_id_by_user("bob")
        self.assertEqual(result4,2)


    def test_handle_user_command_create_user(self):

        #create user
        result=self.UCH.handle_user_command("create user123 password123")
        self.assertTrue(self.UCH.UserMenager.pending_cr_user)
        self.assertIn(self.UCH.UserMenager.pending_cr_password, "password123")
        self.assertIn(result,"Please insert password once again to confirm your password")
        
        result2=self.UCH.handle_user_command("password123")
        self.assertIn("created successfully.",result2)

        result3=self.UCH.UserMenager.get_user_by_id(2)
        self.assertEqual(result3,"bob")

        result4=self.UCH.UserMenager.get_id_by_user("bob")
        self.assertEqual(result4,2)

    def test_check_login_admin_and_fct(self):

        #check admin login
        result1=self.UCH.handle_user_command("login admin")
        self.assertTrue(self.UCH.UserMenager.pending_admin)
        self.assertTrue(self.UCH.UserMenager.pending_user)
        self.assertIn(result1,"You are trying login as an admin, please insert password")

        #check admin password
        result1=self.UCH.handle_user_command("admin")
        self.assertTrue(self.UCH.UserMenager.logged_admin)
        self.assertIn("You are logged in as admin",result1)

        #check user list
        result2=self.UCH.handle_user_command("admin_user")
        self.assertIn(("admin",),result2)
        self.assertIn(("bob",),result2)
        self.assertIn(("adam3",),result2)



    def test_check_login_logout_normal_user(self):

        #check existing user
        result2=self.UCH.handle_user_command("login bob")
        self.assertTrue(self.UCH.UserMenager.pending_user)
        self.assertIn(result2,"Login found, please insert password")

        #check bob password
        result1=self.UCH.handle_user_command("bob")
        self.assertTrue(self.UCH.UserMenager.logged_user)
        self.assertTrue(self.UCH.UserMenager.logged_user_id)
        self.assertIn("You are logged in as bob",result1)

        result2=self.UCH.handle_user_command("logout")
        self.assertFalse(self.UCH.UserMenager.logged_user)
        self.assertFalse(self.UCH.UserMenager.logged_user_id)
        self.assertIn("User succesfully logout",result2)


    def test_check_login_not_existing_user(self):

        #check not exisiting user
        result3=self.UCH.handle_user_command("login xyz")
        self.assertFalse(self.UCH.UserMenager.pending_user)
        self.assertFalse(self.UCH.UserMenager.pending_user_id)
        self.assertIn(result3,"User doesn't exist")
        
    def test_check_login_inncorect_password(self):

        #check exisiting user
        result=self.UCH.handle_user_command("login adam3")
        self.assertTrue(self.UCH.UserMenager.pending_user)
        self.assertTrue(self.UCH.UserMenager.pending_user_id)
        self.assertIn(result,"Login found, please insert password")

        #check inccorect
        result1=self.UCH.handle_user_command("xxxzxzx")
        self.assertFalse(self.UCH.UserMenager.logged_user)
        self.assertFalse(self.UCH.UserMenager.logged_user_id)
        self.assertIn("Incorrect password",result1)

    def test_change_password(self):

        #check exisiting user
        result=self.UCH.handle_user_command("login adam3")
        self.assertTrue(self.UCH.UserMenager.pending_user)
        self.assertTrue(self.UCH.UserMenager.pending_user_id)
        self.assertIn(result,"Login found, please insert password")

        #check login
        result1=self.UCH.handle_user_command("adam3")
        self.assertTrue(self.UCH.UserMenager.logged_user)
        self.assertTrue(self.UCH.UserMenager.logged_user_id)
        self.assertIn("You are logged in as adam3",result1)

        result2=self.UCH.handle_user_command("pw_change new_passoword")
        self.assertIn("Please enter password once again",result2)

        result3=self.UCH.handle_user_command("new_passoword")
        self.assertIn("Password has been changed" ,result3)

        #logout for test new password
        result4=self.UCH.handle_user_command("logout")
        print(result4)
        self.assertIn("User succesfully logout" ,result4)

        #login with a new password
        self.UCH.handle_user_command("login adam3")
        self.assertTrue(self.UCH.UserMenager.pending_user)
        self.assertTrue(self.UCH.UserMenager.pending_user_id)
        result = self.UCH.handle_user_command("new_passoword")
        self.assertIn("You are logged in as adam3", result)

    def test_status(self):

        result=self.UCH.UserMenager.status()
        self.assertIn("You are not logged in, please type your login and password",result)

        #check exisiting user
        result=self.UCH.handle_user_command("login adam3")
        self.assertTrue(self.UCH.UserMenager.pending_user)
        self.assertTrue(self.UCH.UserMenager.pending_user_id)
        self.assertIn(result,"Login found, please insert password")

        #check login with changed password because SetUpClase has chaned Table in Database
        result1=self.UCH.handle_user_command("adam3")
        self.assertTrue(self.UCH.UserMenager.logged_user)
        self.assertTrue(self.UCH.UserMenager.logged_user_id)
        self.assertIn("You are logged in as adam3",result1)

        result=self.UCH.UserMenager.status()
        self.assertIn("You are already logged in as adam3",result)

