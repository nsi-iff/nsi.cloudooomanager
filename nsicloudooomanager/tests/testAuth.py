import unittest
from os.path import abspath, dirname, join
from nsivideoconvert.auth import Authentication

FOLDER_PATH = abspath(dirname(__file__))

class TestAuth(unittest.TestCase):

    def setUp(self):
        self.auth = Authentication(join(FOLDER_PATH,"output", "authenticate.db"))
        self.auth.add_user("test", "pass_user")

    def test_add_user(self):
        self.assertEquals(self.auth.authenticate("test", "pass_user"), True)

    def test_del_user(self):
        self.auth.del_user("test")
        self.assertFalse(self.auth.authenticate("test", "pass_user"))

    def test_authenticate(self):
        self.assertEquals(self.auth.authenticate("test", "pass_userx"), False)
        self.assertEquals(self.auth.authenticate("test", "pass_user"), True)

    def tearDown(self):
        self.auth.del_user("test")

if __name__ == "__main__":
    unittest.main()

