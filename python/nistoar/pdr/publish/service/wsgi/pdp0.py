"""
WSGI support for the pdp0 convention of the Programmatic Data Publishing (PDP) service
"""
import logging, json
from collections import OrderedDict
from typing import Callable
from urllib.parse import parse_qs

from .base import SubApp, Handler
from .. import PDP0Service, status
from ...prov import PubAgent, Action
from ... import (PublishingStateException, SIPNotFoundError, BadSIPInputError, NERDError,
                 SIPStateException, SIPConflictError, UnauthorizedPublishingRequest)
from nistoar.pdr.utils.webrecord import WebRecorder
from nistoar.nerdm.validate import ValidationError

class PDP0App(SubApp):
    """
    The WSGI SubApp that handles the pdp0 convention of the PDP service
    """

    def __init__(self, parentlog, config={}):
        convention = config.get('convention', 'pdp0')
        super(PDP0App, self).__init__(convention, parentlog.getChild(convention), config)

        self.svc = PDP0Service(self.cfg, convention)   # IngestService?
        self.statuscfg = {"cachedir": self.svc.statusdir}

    def sips_for(self, who: PubAgent):
        if who.group is None:
            return []
        return status.SIPStatus.requests(self.statuscfg, who.group)
        
    def create_handler(self, env: dict, start_resp: Callable, path: str, who: PubAgent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        """
        return self._Handler(self, path, env, start_resp, who)

    class _Handler(Handler):
        default_agent = None
        ACTION_UPDATE   = ''
        ACTION_FINALIZE = "finalize"
        ACTION_PUBLISH  = "publish"

        def __init__(self, app, path: str, wsgienv: dict, start_resp: Callable, who=None, config: dict={}):
            self._app = app
            Handler.__init__(self, path, wsgienv, start_resp, who, config, self._app.log)
            self._reqrec = None
            if self._app._recorder:
                self._reqrec = self._app._recorder.from_wsgi(self._env)

        def get_action(self):
            """
            return the value of the action query parameter of None if it was not provided
            """
            qstr = self._env.get('QUERY_STRING')
            if not qstr:
                return self.ACTION_UPDATE

            params = parse_qs(qstr)
            action = params.get('action')
            if len(action) > 0 and action[0] in [self.ACTION_FINALIZE, self.ACTION_PUBLISH]:
                return action[0]
            return self.ACTION_UPDATE

        def do_GET(self, path, ashead=False):
            path = path.lstrip('/')
            if not self.authorize():
                return self.send_unauthorized()

            if not path:
                return self.send_json(self._app.sips_for(self.who), ashead=ashead)

            parts = path.split('/', 1)
            try:
                # need to check authorization
                stat = self._app.svc.status_of(parts[0])
                if not stat.any_authorized(self.who.group) or stat.state == status.NOT_FOUND:
                    return self.send_error(404, "Authorized SIP Not Found")

                out = self._app.svc.describe(path)
                out['pdr:status'] = stat.state
                if out.get('pdr:message') is not None:
                    out['pdr:message'] = statis/user_message[stat.state]
                return self.send_json(out, ashead=ashead)

            except SIPNotFoundError as ex:
                return self.send_error(404, "Not Found")

            except Exception as ex:
                self.log.exception("Failed to describe SIP: %s: %s", parts[0], str(ex))
                return self.send_error(500, "Server error")

        def do_POST(self, path):
            path = path.lstrip('/')
            if not self.authorize():
                return self.send_unauthorized()

            sipid = ''
            stat = None
            if len(path.split('/', 1)) > 1:
                # cannot POST to paths that appear to point to SIP components
                return self.send_error(405, "Method not allowed on this resource")
            if path:
                sipid = path
                stat = self._app.svc.status_of(path)
                if stat.state == status.NOT_FOUND:
                    # SIP must first be created before a component can be added
                    return self.send_error(404, "SIP not found")

            action = self.get_action()

            try:
                bodyin = self._env.get('wsgi.input')
                if bodyin is None:
                    if self._reqrec:
                        self._reqrec.record()
                    return self.send_error(400, "Missing input NERDm document")
                if self.log.isEnabledFor(logging.DEBUG) or self._reqrec:
                    body = bodyin.read()
                    nerdm = json.loads(body, object_pairs_hook=OrderedDict)
                else:
                    nerdm = json.load(bodyin, object_pairs_hook=OrderedDict)
                if self._reqrec:
                    self._reqrec.add_body_text(json.dumps(pod, indent=2)).record()

            except (ValueError, TypeError) as ex:
                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.error("Failed to parse input: %s", str(ex))
                    self.log.debug("\n%s", body)
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                return self.send_error(400, "Input not parseable as JSON")

            except Exception as ex:
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                raise

            try:
                success = 200
                if sipid:
                    if not stat.any_authorized(self.who.group):
                        self.info("%s is not authorized to update SIP, %s", self.who.actor, sipid)
                        return self.send_unauthorized()

                    # this is a request to add a component
                    cmpid = self._app.svc.upsert_component_metadata(sipid, nerdm, self.who)

                    out = None
                    if action == self.ACTION_FINALIZE or action == self.ACTION_PUBLISH:
                        self._app.svc.finalize(sipid, self.who)
                        out = self._app.svc.describe("{}/{}".format(sipid, cmpid))

                        if action == self.ACTION_PUBLISH:
                            self._app.svc.publish(sipid, self.who)

                    if not out:
                        out = self._app.svc.describe("{}/{}".format(sipid, cmpid))

                else:
                    # this is a request to create an SIP
                    sipid = self._app.svc.accept_resource_metadata(nerdm, self.who, create=True)
                    if not stat or stat.state == status.NOT_FOUND:
                        success = 201

                    # Is there an accompanying request to finalize or publish?
                    out = None
                    if action == self.ACTION_FINALIZE or action == self.ACTION_PUBLISH:
                        self._app.svc.finalize(sipid, self.who)
                        out = self._app.svc.describe(sipid)

                        if action == self.ACTION_PUBLISH:
                            self._app.svc.publish(sipid, self.who)

                    if not out:
                        out = self._app.svc.describe(sipid)

                stat = self._app.svc.status_of(sipid)
                out['pdr:status'] = stat.state
                if out.get('pdr:message') is not None:
                    out['pdr:message'] = statis/user_message[stat.state]
                return self.send_json(out, code=success)

            except NERDError as ex:
                self.log.error("Bad NERDm data POSTed to %s: %s", path, str(ex))
                self.send_error(400, "Bad Input NERDm data", str(ex)+"\n", "text/plain")

            except ValidationError as ex:
                self.log.error("Invalid NERDm data POSTed to %s: %s", path, str(ex))
                self.send_error(400, "Bad Input NERDm data", str(ex)+"\n", "text/plain")

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized create request to %s: %s", path, str(ex))
                return self.send_unauthorized()

            except PublishingStateException as ex:
                msg = "Attempt to update SIP in un-update-able state: %s" % str(ex)
                self.log.error(msg)
                self.send_error(409, "Conflicting SIP state", msg+"\n", "text/plain")

            except Exception as ex:
                if sipid:
                    self.log.exception("Failed to accept SIP: %s: %s", sipid, str(ex))
                else:
                    self.log.exception("Failed to accept SIP: %s", str(ex))
                return self.send_error(500, "Server error")
                
        def do_PUT(self, path):
            path = path.lstrip('/')
            if not self.authorize():
                return self.send_unauthorized()

            if not path:
                return self.send_error(405, "Method not allowed")

            parts = path.split('/', 1)
            sipid = parts[0]
            compid = None
            if len(parts) > 1:
                compid = parts[1]

            stat = self._app.svc.status_of(sipid)
            if stat.state != status.NOT_FOUND and not stat.any_authorized(self.who.group):
                self.log.info("%s is not authorized to update SIP, %s", self.who.actor, sipid)
                return self.send_unauthorized()

            action = self.get_action()

            try:
                bodyin = self._env.get('wsgi.input')
                if bodyin is None:
                    if self._reqrec:
                        self._reqrec.record()
                    return self.send_error(400, "Missing input NERDm document")
                if self.log.isEnabledFor(logging.DEBUG) or self._reqrec:
                    body = bodyin.read()
                    nerdm = json.loads(body, object_pairs_hook=OrderedDict)
                else:
                    nerdm = json.load(bodyin, object_pairs_hook=OrderedDict)
                if self._reqrec:
                    self._reqrec.add_body_text(json.dumps(pod, indent=2)).record()

            except (ValueError, TypeError) as ex:
                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.error("Failed to parse input: %s", str(ex))
                    self.log.debug("\n%s", body)
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                return self.send_error(400, "Input not parseable as JSON")

            except Exception as ex:
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                raise

            try:
                if compid:
                    nerdm['@id'] = compid
                    self._app.svc.upsert_component_metadata(sipid, nerdm, self.who)

                    out = None
                    if action == self.ACTION_FINALIZE or action == self.ACTION_PUBLISH:
                        self._app.svc.finalize(sipid, self.who)
                        out = self._app.svc.describe("{}/{}".format(sipid, cmpid))

                        if action == self.ACTION_PUBLISH:
                            self._app.svc.publish(sipid, self.who)

                    if not out:
                        out = self._app.svc.describe("{}/{}".format(sipid, cmpid))

                else:
                    self._app.svc.accept_resource_metadata(nerdm, self.who, sipid,
                                                           stat.state == status.NOT_FOUND)

                    out = None
                    if action == self.ACTION_FINALIZE or action == self.ACTION_PUBLISH:
                        self._app.svc.finalize(sipid, self.who)
                        out = self._app.svc.describe(sipid)

                        if action == self.ACTION_PUBLISH:
                            self._app.svc.publish(sipid, self.who)

                    if not out:
                        out = self._app.svc.describe(sipid)

                stat = self._app.svc.status_of(sipid)
                out['pdr:status'] = stat.state
                if out.get('pdr:message') is not None:
                    out['pdr:message'] = statis/user_message[stat.state]
                return self.send_json(out)

            except NERDError as ex:
                self.log.error("Bad NERDm data PUT to %s: %s", path, str(ex))
                return self.send_error(400, "Bad Input NERDm data", str(ex)+"\n", "text/plain")

            except ValidationError as ex:
                self.log.error("Invalid NERDm data PUT to %s: %s", path, str(ex))
                self.send_error(400, "Bad Input NERDm data", str(ex)+"\n", "text/plain")

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized update request to %s: %s", path, str(ex))
                return self.send_unauthorized()

            except PublishingStateException as ex:
                msg = "Attempt to update SIP in un-update-able state: %s" % str(ex)
                self.log.error(msg)
                return self.send_error(409, "Conflicting SIP state", msg+"\n", "text/plain")

            except Exception as ex:
                self.log.exception("Failed to accept SIP update: %s: %s", path, str(ex))
                return self.send_error(500, "Server error")

        def do_DELETE(self, path):
            path = path.lstrip('/')
            if not self.authorize():
                return self.send_unauthorized()

            if not path:
                return self.send_error(405, "Method not allowed")

            parts = path.split('/', 1)
            sipid = parts[0]
            compid = None
            if len(parts) > 1:
                compid = parts[1]

            stat = self._app.svc.status_of(sipid)
            if stat.state == status.NOT_FOUND:
                return self.send_error(404, "SIP not found")

            if not stat.any_authorized(self.who.group):
                self.info("%s is not authorized to update SIP, %s", self.who.actor, sipid)
                return self.send_unauthorized()

            try:
                if compid:
                    try:

                        self._app.svc.remove_component(sipid, compid, self.who)

                    except SIPConflictError as ex:
                        self.log.error("%s: unable to remove component, %s: %s", sipid, compid, str(ex))
                        return self.send_error(409, "Not in DELETEable state")

                else:
                    try:

                        self._app.svc.delete(sipid, self.who)
    
                    except SIPConflictError as ex:
                        self.log.error("%s: unable to delete SIP: %s", sipid, compid, str(ex))
                        return self.send_error(409, "Not in DELETEable state")

            except SIPNotFoundError as ex:
                return self.send_error(404, "SIP Not Found")

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized request to delete %s: %s", path, str(ex))
                return self.send_unauthorized()

            except Exception as ex:
                self.log.exception("Failed to DELETE %s: %s", path, str(ex))
                return self.send_error(500, "Server error")

            return self.send_ok()
            
        def do_PATCH(self, path):
            # This method is only used to finalize or publish
            path = path.lstrip('/')
            if not self.authorize():
                return self.send_unauthorized()

            parts = path.split('/')
            if not path or len(parts) > 1:
                return self.send_error(405, "Method not allowed")
            sipid = parts[0]

            action = self.get_action()

            try:
                bodyin = self._env.get('wsgi.input')
                if bodyin:
                   body = bodyin.read(1)
                   if len(body) != 0:
                       return self.send_error(400, "Body not allowed")

                out = None
                if action == self.ACTION_FINALIZE or action == self.ACTION_PUBLISH:
                    self._app.svc.finalize(sipid, self.who)
                    out = self._app.svc.describe(sipid)

                    if action == self.ACTION_PUBLISH:
                        self._app.svc.publish(sipid, self.who)

                if not out:
                    out = self._app.svc.describe(sipid)

                stat = self._app.svc.status_of(sipid)
                out['pdr:status'] = stat.state
                if out.get('pdr:message') is not None:
                    out['pdr:message'] = statis/user_message[stat.state]
                return self.send_json(out)

            except SIPNotFoundError as ex:
                return self.send_error(404, "SIP Not Found")

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized %s request on %s: %s", action, path, str(ex))
                return self.send_unauthorized()

            except Exception as ex:
                self.log.exception("Failed to take %s action on %s: %s", action, path, str(ex))
                return self.send_error(500, "Server error")


            
