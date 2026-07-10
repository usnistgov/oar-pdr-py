"""
A module that provides methods of authentication that can be used by web service implementations.
"""
from typing import Mapping, List, Callable
from logging import Logger

import jwt

from nistoar.pdr.utils.prov import Agent

class Unauthenticated(Exception):
    """
    An exception indicating that a service client did not successfully authenticate itself.
    This may be because the credentials are required but none were provided by the client, or 
    because the credentials presented were not valid.

    Note that an implementation is not required to raise this exception, particularly if 
    credentials are optional.  Instead an identity can be returned that specifically represent
    an unauthenticated client.  
    """
    pass

def authenticate_via_authkey(svcname: str, env: Mapping, authcfg: Mapping, log: Logger,
                             agents: List[str]=None, client_id: str=None) -> Agent:
    """
    authenticate the user via a simple shared Bearer Authorization key.

    This authorization method simply requires the client to present an opaque key set as a
    Bearer token to the Authorization HTTP header.  The key must be provided in the given 
    configuration within the ``authorized`` object which contains a list of client authentication 
    objects; each object contains the following parameters:

    ``auth_key``
       _str_ (required).  A recognized opaque key looked for as a Bearer Authorization token

    ``user``
       _str_ (required).  an identifier to set the returned Agent ``actor`` id to when the client 
       presents the associated ``auth_key``.  

    ``type``
       _str_ (optional).  the name of the type classification for the Agent identity.  The value
       should either be "user" or "auto", where the former indicates that a real human interaction
       initiated the current request and the latter indicates an automated process.  The default 
       will be "auto" (since auth keys are not typically secure enough for human-initiated requests).

    ``class``
       _str_ (optional).  the label to set the Agent's class to which defines the permissions the 
       agent has.  If not provided, it will default to the value of the ``client`` parameter.  

    ``client``
       _str_ (recommended).  a name for the client; this will be set as the Agent's client_id (and,
       thus, the most recent delegated agent).  It is also used as a default Agent class if ``class``
       is not provided.


    :param str   svcname: a name to provide as the agent software vehicle
    :param dict      env: the WSGI environment containing the request data
    :param dict  authcfg: the supported keys and user configuration (see above)
    :param Logger    log: the logger that can be used to record messages
    :param [str]  agents: an optional list of agent strings to attach to output agent
    :param str client_id: an ID representing the OAR client being used to connect.  If None,
                          either an ID was not provided or is otherwise not supported by the 
                          app.  This will be over-ridden by a configured value in the returned
                          Agent if provided in the configuration.  
    :returns:  an :py:class:`Agent` instance representing the user
    """
    if not client_id:
        client_id = "(unknown)"
    if not svcname:
        svnname = "nistoar"
    agents = list(agents) if agents else []

    auth = env.get('HTTP_AUTHORIZATION', "x").split()
    if len(auth) < 2 or auth[0] != "Bearer" or not auth[1]:
        log.warning("Client %s did not provide a Bearer authentication token", str(client_id))
        if authcfg.get('raise_on_anonymous'):
            raise Unauthenticated("No bearer token provided")
        if client_id:
            agents.append(str(client_id))
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)

    for client in authcfg.get('authorized'):
        if client.get("auth_key") == auth[1]:
            clid = client.get('client')
            agcls = client.get('class', clid or Agent.PUBLIC)
            agents.append(clid or client_id)
            tp = client.get('type', Agent.AUTO)
            out = Agent(svcname, tp, client.get('user','authorized'), agcls, agents)
            if clid:
                out.set_prop('client_id', clid)
            return out

    log.warning("Unrecognized token from client %s", str(client_id))
    if authcfg.get('raise_on_invalid'):
        raise Unauthenticated("Unrecognized auth token")
    if client_id:
        agents.append(str(client_id))
    return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents,
                 invalid_reason="Unrecognized auth token")

def authenticate_via_proxy_x509(svcname: str, env: Mapping, authcfg: Mapping, log: Logger,
                                agents: List[str]=None, client_id: str=None) -> Agent:
    """
    authenticate the user assuming that the client provided an X.509 client certificate
    that was validated by the service's reverse proxy server.  If the certificate is valid,
    the proxy server will have provided the following HTTP headers:
      * OAR_SSL_S_DN -- set to the distinguished name of the certificate's subject
      * Authorization -- set with a Bearer token with a shared secret, required if a 
                         ``proxy_key`` was provided in the configuration.

    This function will look for the following properties in the provided configuration dictionary:

    ``proxy_key``
        (str) _optional_.  The secret key shared with the proxy server configuration.  If set, 
        the proxy server must set this key as a Bearer token in the Authorization HTTP header
        in order for subject data to be considered valid.  If the keys do not match or is not 
        provided in the input request to this function, an invalid anonymous identity is returned.

    Note that this implementation does not provide a means for determining the "true" client ID.  

    :param str   svcname: a name to provide as the agent software vehicle
    :param dict      env: the WSGI environment containing the request data
    :param dict   jwtcfg: the JWT decoding configuration (see above)
    :param Logger    log: the logger that can be used to record messages
    :param [str]  agents: an optional list of agent strings to attach to output agent
    :param str client_id: an ID representing the OAR client being used to connect.  If None,
                          either an ID was not provided or is otherwise not supported by the 
                          app.  
    :returns:  an :py:class:`Agent` instance representing the user
    """
    if not client_id:
        client_id = "(unknown)"
    if not svcname:
        svnname = "nistoar"
    agents = list(agents) if agents else []
    agents.append(client_id)

    subj = env.get('HTTP_OAR_SSL_S_DN')
    if not subj:
        if authcfg.get('raise_on_anonymous'):
            raise Unauthenticated("OAR_SSL_S_DN not provided")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)

    if authcfg.get('proxy_key'):
        # we're expecting a validation token from the proxy server
        auth = env.get('HTTP_AUTHORIZATION' 'x').split()
        if len(auth) < 2 or auth[0] != "Bearer":
            log.warning("Reverse proxy server did not provide an authentication token")
            if authcfg.get('raise_on_invalid'):
                raise Unauthenticated("required proxy key not provided")
            return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents, 
                         invalid_reason="Missing proxy auth token")
        if authcfg['proxy_key'] != auth[1]:
            log.error("Reverse proxy server presented unrecognized authentication token")
            if authcfg.get('raise_on_invalid'):
                raise Unauthenticated("bad proxy key")
            return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents, 
                         invalid_reason="bad proxy auth token")

    # parse the subject elements to construct an identity
    # TODO!
    return None

def authenticate_via_jwt(svcname: str, env: Mapping, jwtcfg: Mapping, log: Logger,
                         agents: List[str], client_id: str=None,
                         claim_to_agent_func: Callable=None) -> Agent:
    """
    authenticate the remote user assuming a JWT was provided as an Authorization Bearer token.

    This function will look for the following properties in the provided configuration dictionary:

    ``key``
        (str) _required_.  The secret key shared with the token generator (usually a separate 
        service) used to encrypt the token.

    ``algorithm``
        (str) _optional_.  The name of the encryption algorithm to encrypt the token.  Currently, 
        only one value is support (the default): "HS256".

    ``require_expiration``
        (bool) _optional_.  If True (default), any JWT token that does not include an expiration 
        time will be rejected, and the client user will be set to anonymous.

    :param str   svcname: a name to provide as the agent software vehicle
    :param dict      env: the WSGI environment containing the request data
    :param dict   jwtcfg: the JWT decoding configuration (see above)
    :param Logger    log: the logger that can be used to record messages
    :param [str]  agents: an optional list of agent strings to attach to output agent
    :param str client_id: an ID representing the OAR client being used to connect.  If None,
                          either an ID was not provided or is otherwise not supported by the 
                          app.  
    :param function claim_to_agent_func:  a function that takes a JWT claimset dictionary and 
                          returns an Agent instance.  If not provided, 
                          :py:func:`make_agent_from_nistoar_claimset` will be executed.
    :returns:  an :py:class:`Agent` instance representing the user
    """
    if not client_id:
        client_id = "(unknown)"
    if not svcname:
        svnname = "nistoar"
    agents = list(agents) if agents else []

    auth = env.get('HTTP_AUTHORIZATION', "x").split()
    if len(auth) < 2 or auth[0] != "Bearer":
        log.warning("Client %s did not provide an authentication token", str(client_id))
        if jwtcfg.get('raise_on_anonymous'):
            raise Unauthenticated("JWT token not provided")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.PUBLIC, agents)

    try:
        userinfo = jwt.decode(auth[1], jwtcfg.get("key", ""),
                              algorithms=[jwtcfg.get("algorithm", "HS256")])
    except jwt.InvalidTokenError as ex:
        log.warning("Invalid token can not be decoded: %s", str(ex))
        if jwtcfg.get('raise_on_invalid'):
            raise Unauthenticated("Undecodable JWT token")
        return Agent(svcname, Agent.UNKN, Agent.ANONYMOUS, Agent.INVALID, agents,
                     invalid_reason="Invalid token can not be decoded")

    if not claim_to_agent_func:
        claim_to_agent_func = make_agent_from_nistoar_claimset
    out = claim_to_agent_func(svcname, userinfo, log, agents, client_id)

    # make sure the token has an expiration date
    if jwtcfg.get('require_expiration', True) and \
       userinfo.get("agent_type", "user") != "auto" and not userinfo.get('exp'):
        # Note expiration was checked implicitly by the above jwt.decode() call
        log.warning("Rejecting non-expiring token for user %s", userinfo.get('sub', "(unknown)"))
        if jwtcfg.get('raise_on_invalid', True):
            raise Unauthenticated("Non-expiring JWT token")
        return Agent(out.vehicle, out.actor_type, out.actor, Agent.INVALID, agents,
                     invalid_reason=f"non-expiring token rejected")

    return out

def make_agent_from_nistoar_claimset(svcname: str, userinfo: Mapping, log: Logger, agents=None,
                                     client_id: str=None) -> Agent:
    """
    Create an Agent instance representing the end user given a JWT claim set assuming 
    it originated from a NIST-OAR JWT service.

    This implementation will use information encoded in the token for key properties of the output
    agent.  In addition to the actor ID (taken from the token's subject), the token may contain a 
    client_id, the agent type, the agent class.  Other non-standard properties may be transfered 
    over as well.

    :param str   svcname:  a name to provide as the agent software vehicle
    :param dict userinfo:  a dictionary containing the JST claimset data
    :param Logger    log:  a Logger object that should be used to record warning messages
                           (e.g. if the claimset is misisng key data)
    :param list[str] agents:  a list of agents that the user described by the claim set is acting
                           on behalf of.  By default, if None or empty, no agents will be attached 
                           to the returned Agent.
    """
    subj = userinfo.get('sub')
    email = userinfo.get('userEmail')
    group = Agent.PUBLIC
    if not subj:
        log.warning("User token is missing subject identifier; defaulting to anonymous")
        subj = Agent.ANONYMOUS
    elif subj.endswith("@nist.gov"):
        group = "nist"
        subj = subj[:-1*len("@nist.gov")]
    elif email and email.endswith("@nist.gov"):
        group = "nist"

    umd = dict((k,v) for k,v in userinfo.items()
                         if k not in ["userEmail", "sub", "agclass", "vehicle"])

    # token may contain the client_id; if so, use it.
    client_id = umd.get('client_id', client_id)
    if client_id:
        agents.append(client_id)
    agclass = group
    if umd.get('client_id'):
        agclass = umd['client_id'].split(':')[0]  # a unique ID may follow :

    # we allow the token to provide agent information 
    umd.setdefault('actortype', Agent.USER)
    umd.setdefault('agents', agents)
    
    return Agent(vehicle=svcname, actorid=subj, agclass=agclass, groups=[group], email=email, **umd)


