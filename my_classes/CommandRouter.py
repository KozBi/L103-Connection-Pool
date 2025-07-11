from my_classes.MessagingService import MessagingService
from my_classes.UserCommandHandler import UserCommandHandler
from datetime import datetime

class CommandRouter:
    def __init__(self, Version,Creationdate,database):
        self.Version=Version
        self.Creationdate=Creationdate
        self.database=database
        self.UserCommandHandler= UserCommandHandler(self.database)
        self.MessagingService=MessagingService(self.database)

    
    def _help(self):
        return {"help":f"""- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n 
        {self.UserCommandHandler.status()}\n
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n 
        Available command:\n
        'uptime'-return life time of server\n
        'info'-informs about server version\n
        'stop'-close the server\n
        'login |your_login| login to your account.\n
        'logout' you logout from your account\n
        'create 'login' 'password' - a new account create with a passowrd\n
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n 
         |Commands  If you are LOGGED IN in ohter case command is unknow|\n
        'msg' check how many messages you have\n 
        'w' 'receiver' write a message to another user\n 
        'rd' read all message\n 
        'del' 'message_numer' or '-a' - delete specified message or all messages\n 
        'pw_change' 'new_password' change password for your account\n
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n 
         |Commands  If you are LOGGED IN as admin in ohter case command is unknow|\n
        'admin_user' get a list of all users\n 
        'admin_rd' 'username'  read all message for a user\n 
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n""" 
        }

    def handle_command(self,cmd):
        self.UserCommandHandler.status()
        # hanlde commands from class User
        # if login add also information how many messages already loged user has.
        user_respond= self.UserCommandHandler.handle_user_command(cmd)
        if user_respond:
            return user_respond
        
        # hanlde commands from class MessagingService only if user is loged
        if self.UserCommandHandler.UserMenager.logged_user_id:
            # call handle message (command,loged user , send a class to get posibility to get_user_id)
            message_respond=self.MessagingService.handle_message_command(cmd,self.UserCommandHandler.UserMenager)
            if message_respond:
                return message_respond
        
        match str(cmd):
            case "uptime": 
                #calcute server date
                current_time=datetime.now()
                current_time=current_time-self.Creationdate
                current_time= str(current_time).split('.')[0]
                return f"Server uptime: {current_time}"
            case "info": 
                return f"Server version: {self.Version}"
            case "help": 
                    return self._help()
            case "stop": 
                pass
            case _: #deafult case
                return "Uknown command, try 'help"