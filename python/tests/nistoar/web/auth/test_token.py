import os, json, pdb, time, jwt
import unittest as test
from collections import OrderedDict
from io import StringIO

from nistoar.web.auth import token as auth
from nistoar.base.config import ConfigurationException

class TestJWTGenerator(test.TestCase):

    def setUp(self):
        auth.default_token_generator = None
        self.cfg = {
            "secret": "hush!"
        }

        self.gen = auth.JWTGenerator(self.cfg)

    def test_ctor(self):
        self.assertEqual(self.gen._secret, self.cfg['secret'])
        self.assertEqual(self.gen.lifetime, 3600)

        self.cfg['lifetime'] = 600
        self.gen = auth.JWTGenerator(self.cfg)
        self.assertEqual(self.gen._secret, self.cfg['secret'])
        self.assertEqual(self.gen.lifetime, 600)

        with self.assertRaises(ConfigurationException):
            auth.JWTGenerator({})
        with self.assertRaises(ConfigurationException):
            auth.JWTGenerator({"lifetime": "tomorrow", "secret": "XX"})

    def test_generate(self):
        due = time.time() + 3600
        tok = self.gen.generate("me", {"name": "Bud", "color": "green"})
        self.assertIsNotNone(tok)

        data = jwt.decode(tok, self.cfg['secret'], algorithms="HS256")
        self.assertEqual(data['sub'], "me")
        self.assertGreater(data['exp'], due-1)
        self.assertLess(data['exp'], due+5)
        self.assertEqual(data['name'], "Bud")
        self.assertEqual(data['color'], "green")

        due = time.time() + 600
        tok = self.gen.generate("you",
                                {"sub": "me", "phase": "Bud", "color": "blue"},
                                600)
        self.assertIsNotNone(tok)

        data = jwt.decode(tok, self.cfg['secret'], algorithms="HS256")
        self.assertEqual(data['sub'], "you")
        self.assertGreater(data['exp'], due-1)
        self.assertLess(data['exp'], due+5)
        self.assertEqual(data['phase'], "Bud")
        self.assertEqual(data['color'], "blue")

    def test_create_default_token_generator(self):
        self.assertIsNone(auth.default_token_generator)
        auth.create_default_token_generator(self.cfg)
        self.assertTrue(isinstance(auth.default_token_generator,
                                   auth.JWTGenerator))

        
                         
if __name__ == '__main__':
    test.main()
        
