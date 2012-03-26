import unittest
from nsicloudooomanager.interfaces.auth import IAuth
from nsicloudooomanager.interfaces.http import IHttp
from nsicloudooomanager.auth import Authentication
from nsicloudooomanager.http import HttpHandler

class TestInterface(unittest.TestCase):

    def test_auth(self):
        self.assertEquals(IAuth.implementedBy(Authentication), True)
        self.assertEquals(sorted(IAuth.names()), ['add_user',
                                                'authenticate',
                                                'del_user'])

    def test_handler(self):
        self.assertEquals(IHttp.implementedBy(HttpHandler), True)
        self.assertEquals(sorted(IHttp.names()), ['get',
                                                  'get_current_user',
                                                  'post',])

if __name__ == "__main__":
    unittest.main()

