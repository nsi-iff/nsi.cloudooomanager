from os.path import exists
from zope.interface import implements
from nsi.cloudooomanager.interfaces.auth import IAuth
from argparse import ArgumentParser
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

    def add_user(self):
       connection = sqlite3.connect(self.db)
       cursor = connection.cursor()
       db_dict = self._load_db_as_dict()
       dict_user = {}
       parser = ArgumentParser()
       parser.add_argument('--user', dest='user',
                           help="Name for user")
       parser.add_argument('--password', dest='password',
                           help="Password for acess")
       namespace = parser.parse_args()
       if db_dict.has_key(namespace.user):
          return False
       else:
          cursor.execute('''insert into clientes values(?, ?, ?, ?, ?)''', 
              (None, None, None, namespace.user, namespace.password))
          connection.commit()
          dict_user[namespace.user] = namespace.password
          cursor.close()

       return True

    def del_user(self):
       connection = sqlite3.connect(self.db)
       cursor = connection.cursor()
       db_dict = self._load_db_as_dict()
       parser = ArgumentParser()
       parser.add_argument('--user', dest='user',
                           help="Name for user")
       parser.add_argument('--password', dest='password',
                           help="Password for acess")
       namespace = parser.parse_args()
       if not db_dict.has_key(namespace.user):
          return False
       else:
          cursor.execute('''delete from clientes where usuario = ?''', (namespace.user,))
          connection.commit()
          cursor.close()
       return True

    def authenticate(self, user, password):
        db_dict = self._load_db_as_dict()
        parser = ArgumentParser()
        parser.add_argument('--user', dest='user',
                            help="Name for user")
        parser.add_argument('--password', dest='password',
                            help="Password for acess")
        namespace = parser.parse_args()
        if not db_dict.has_key(namespace.user):
          return False
        elif db_dict[namespace.user] == namespace.password:
          return True
        else:
          return False

