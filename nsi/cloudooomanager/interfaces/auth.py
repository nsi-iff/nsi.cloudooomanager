from zope.interface import Interface

class IAuth(Interface):
    
    def add_user(user, passwd):
        """Add user in user list"""

    def del_user(user):
        """Delete user"""

    def authenticate(user, passwd):
        """Authenticate the user. If the user not exists or the password is
        wrong, returns False. Otherwise, returns True."""
