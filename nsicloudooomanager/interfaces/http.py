from zope.interface import Interface

class IHttp(Interface):

    def get_current_user():
        """Returns the user and password of request"""

    def get():
        """ """
    def post():
        """ """
