from my_classes.UserMenager import UserMenager as UM
from my_classes.DataBaseService import DataBaseService
import os


class MessagingService():
    def __init__(self,database:DataBaseService):
        self.database=database 
        self._write_m_tpl=None #wrtie message tuple (user_id,_receiver)# - help variable for write message

        self.max_messages=5

    def _load_messages(self,id):
        return self.database.load_message(id)

    def _count_message_num(self,user_id):
        return self.database.msg_count(user_id)

    def number_message(self,user_id):
            _num=self._count_message_num(user_id)   
            if _num<self.max_messages:
                return f"You have {_num} messages" 
            elif _num==0:
                return "You don't have messages" 
            else: return f"You have {_num} messages. Your box messages is full. Please delete messages using del command"

    def read_message_all(self,Umenager:UM,user_id=None):
        if not user_id:
            user_id=Umenager.logged_user_id
        try:
            messages=self._load_messages(user_id)
            msgs="\n" 
            if messages:
        #     u_message=messages[str(user_id)]
                for m in messages:    
                    #String concatenation
                    msgs+= f"{m}\n"
                if len(messages)>=self.max_messages:
                    return f"{msgs} +  Your's box messages is full. Please delete messages using del command"
                else: return msgs   
            else: return "You dont have any messages"
        except(KeyError): return f"This user doesn't exist:{user_id}"
        
    def receiver_found(self,receiver,Umenager:UM):
        _receiverid=Umenager.get_id_by_user(receiver)
        user_id=Umenager.logged_user_id
        if _receiverid:
            self._write_m_tpl=(user_id,_receiverid)
            return f"User: {receiver} found, please type a message --- max 255 of characters"
        else:
            self._write_m_tpl=None
            return f"{receiver} not found in userlist"
 
    def write_message(self,sender_id:int,receiver_id:int,message:str,Umenager:UM):
        #check if the messesage is not too long
        if len(message) > 255:
            self._write_m_tpl=None
            return "Message too long (max 255 characters)."
        # find id
        self._write_m_tpl=None
        # count message 
        if self._count_message_num(receiver_id)>=5:
            return "The recipient's message box is full. You cannot send a message"
        # try to create a message
        if  self.database.write_message(receiver_id,sender_id,message):        
            return "Message sent"
        else: return "Something goes wrong"       
            

    def delete_message(self,id:int,Umenager:UM,username=None):
        if not username:
            username=Umenager.logged_user_id
        if id=="-a":
            if self.database.delete_all_message(username):
                return f"All messages has been deleted"
            else: return "Something goes wrong"   
        else:
            if self.database.delete_one_message(id):
                return f"Message {id} has been deleted"
            else: return "Something goes wrong"

    def handle_message_command(self,command,UserMenage:UM):

        if self._write_m_tpl:
            return self.write_message(self._write_m_tpl[0],self._write_m_tpl[1],command,UserMenage)

        parts=command.split() #split string
        
        if parts[0]=="msg" and UserMenage.logged_user_id:
            return f"Nummber of messages for {UserMenage.logged_user} : {self.number_message(UserMenage.logged_user_id)}"
        
        if parts[0]=="rd":
            return self.read_message_all(UserMenage)
        
        if parts[0]=="w" and len(parts)>1:
                return self.receiver_found(parts[1],UserMenage)
        elif parts[0]=="w": return "Please specify a user to write to"
        
        if parts[0]=="del" and len(parts)>1:
            return self.delete_message(parts[1],UserMenage)
        elif parts[0]=="del":
            return "Wrong parameter with command 'del', please enter message number also ['-a' delete all message]"
        
        #admin accees:
        if UserMenage.logged_admin:
            
            if parts[0]=="admin_rd" and len(parts)>1:
                who=UserMenage.get_id_by_user(parts[1])
                if who:
                    return self.read_message_all(UserMenage,who)
                else:
                    return "Please eneter correct username -- admin_rd 'username'"
            elif parts[0]=="admin_rd":
                return "Please eneter username -- admin_rd 'username'"
            
            if parts[0]=="admin_del" and len(parts)>2:
                who=UserMenage.get_id_by_user(parts[1])
                if who:
                    result=self.delete_message(parts[2],UserMenage,who)
                    return result
                else: return "This user doesn't exist"
            elif parts[0]=="admin_del":
                return "admin_del-- admin_del 'username' 'message_number_to_delete - ['-a' delete all message]"
        
        else:
            return None