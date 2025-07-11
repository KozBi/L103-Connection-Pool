from my_classes.UserMenager import  UserMenager
class UserCommandHandler:
    def __init__(self,database):
        self.database=database
        self.UserMenager=UserMenager(database=self.database)

    def handle_user_command(self, cmd:str):
      
        parts=cmd.split() #split string

        if not self.UserMenager.logged_user:

            #check pennding password in account create process
            if self.UserMenager.pending_cr_password:
                return self.UserMenager.create_user_passw(cmd)
                
            # check create a new user
            if len(parts)==3 and parts[0]== "create":  # if 3 strings and first "create" check user in json file        
                return self.UserMenager.create_user(parts[1], parts[2])
            
            # check login
            if not self.UserMenager.pending_user:
                parts=cmd.split() #split string
                if len(parts)==2 and parts[0]== "login":  # if 2 strings and first "login" check user in json file        
                    return self.UserMenager.check_login(parts[1])
                
            # check password
            if self.UserMenager.pending_user:     
                    return self.UserMenager.check_password(cmd) 
            
        #password change
        if parts[0]=="pw_change" and len(parts)>1:
           return  self.UserMenager.change_password(parts[1])   
        elif self.UserMenager._pending_password_change:
            return  self.UserMenager.change_password(parts[0])  
        elif parts[0]=="pw_change":
            return "Pease enter a new password as a second argument"
        aa=parts[0] 
        print(parts[0])
        if parts[0]=="logout": 
                  
            return self.UserMenager.logout()
        
        if self.UserMenager.logged_admin:

            if parts[0]=="admin_user":
                return self.UserMenager.admin_list_users()
        
        # command not related to this class
        return None     

    def status(self):
        return self.UserMenager.status()