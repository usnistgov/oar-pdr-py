"""
a module for creating and managing JWT tokens
"""
# Note: this code is copied directly from nistoar.auth.creds (from
# https://github.com/usnistgov/oar-auth-py).  It should be removed if/when
# nistoar.auth gets properly integrated into this repository (either as a
# dependency or a submodule).
import time
from typing import Mapping
from abc import ABC, abstractmethod, abstractproperty

import jwt

from nistoar.base.config import ConfigurationException

class TokenGenerator(ABC):
    """
    a class that generates authentication tokens 
    """
    def __init__(self, config: Mapping):
        """
        configure this token generator.  The supported parameters depend on
        the concrete implemenetation.  
        """
        if not isinstance(config, Mapping):
            raise TypeError("TokenGenerator.init: config parameter not a "
                            "dictionary")
        self.cfg = config

    @abstractproperty
    def lifetime(self):
        """
        the default time in seconds before a generated tokens will expire and 
        no longer be valid.  This can be overridden via :py:meth:`generate`.  
        """
        raise NotImplemented()

    @abstractmethod
    def generate(self, subject: str, data: Mapping, lifetime=None) -> str:
        """
        generate the token based on the given data
        :param str  subject:  the subject (i.e. user ID) of the credential
        :param dict    data:  the data to encode into the token
        :param int lifetime:  the time in seconds until the token should expire.
                              If not given, a configured default will be used.
                              An implementation may ignore this value.
        """
        raise NotImplemented()

class JWTGenerator(TokenGenerator):
    """
    a JSON Web Token (JWT) generator.  
    """

    def __init__(self, config):
        if config is None:
            config = {}
        super(JWTGenerator, self).__init__(config)
        self._secret = self.cfg.get('secret')
        if not self._secret:
            raise ConfigurationException("missing or empty parameter: secret")
        self._life = self.cfg.get('lifetime', 3600)  # default: 1 hour
        if not isinstance(self._life, int):
            raise ConfigurationException("wrong type for parameter: lifetime: "
                                         "not an int")

    @property
    def lifetime(self):
        """
        the default time in seconds before a generated tokens will expire and 
        no longer be valid.  This can be overridden via :py:meth:`generate`.  
        """
        return self._life

    def generate(self, subject: str, data: Mapping, lifetime=None) -> str:
        """
        generate the token based on the given data
        :param str  subject:  the subject (i.e. user ID) of the credential
        :param dict    data:  the data to encode into the token
        :param int lifetime:  the time in seconds until the token should expire.
                              If not given, the configured default will be used.
        """
        if not lifetime:
            lifetime = self.lifetime
        if not isinstance(lifetime, int):
            raise TypeError("JWTGenerator.generate: lifetime not an int")
        claimset = dict(data)
        if 'token' in claimset:
            del claimset['token']
        if 'userId' in claimset:
            del claimset['userId']
        claimset['sub'] = subject
        claimset['exp'] = int(time.time() + lifetime)

        return jwt.encode(claimset, self._secret, algorithm="HS256")

default_token_generator = None
default_token_generator_cls = JWTGenerator

def create_default_token_generator(config: Mapping):
    """
    create the TokenGenerator that should be used when a specific 
    instance is not otherwise specified.  The default class is set to 
    JWTGenerator.  The created instance will be saved to this module 
    as the default (``default_token_generator``).
    @param dict config:  the configuration data to pass to the generator 
                         constructor.
    """
    global default_token_generator
    default_token_generator = default_token_generator_cls(config)
    return default_token_generator


