from os.path import exists
from zope.interface import implements
from nsicloudooomanager.interfaces.auth import IAuth
import sqlite3

class Authentication(object):

    implements(IAuth)

    def __init__(self, db):
       self.db = db
       if not exists(db):
          connection = sqlite3.connect(db)
          cursor = connection.cursor()
          cursor.execute('''create table clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, nome varchar(30), descricao varchar(100), usuario varchar(30), password varchar(30))''')

    def _load_db_as_dict(self):
       connection = sqlite3.connect(self.db)
       cursor = connection.cursor()
       result = cursor.execute('''select usuario, password from clientes''')
       dict_user = {}
       for user, password in result:
         dict_user[user] = password
       cursor.close()
       return dict_user

    def add_user(self, user, password):
       connection = sqlite3.connect(self.db)
       cursor = connection.cursor()
       db_dict = self._load_db_as_dict()
       dict_user = {}
       if db_dict.has_key(user):
          return False
       else:
          cursor.execute('''insert into clientes values(?, ?, ?, ?, ?)''', (None, None, None, user, password))
          connection.commit()
          dict_user[user] = password
          cursor.close()

       return True

    def del_user(self, user):
       connection = sqlite3.connect(self.db)
       cursor = connection.cursor()
       db_dict = self._load_db_as_dict()
       if not db_dict.has_key(user):
          return False
       else:
          cursor.execute('''delete from clientes where usuario = ?''', (user,))
          connection.commit()
          cursor.close()
       return True

    def authenticate(self, user, password):
        db_dict = self._load_db_as_dict()
        if not db_dict.has_key(user):
          return False
        elif db_dict[user] == password:
          return True
        else:
          return False

