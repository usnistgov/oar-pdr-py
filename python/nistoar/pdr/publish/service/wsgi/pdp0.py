"""
WSGI support for the pdp0 and pdp1 conventions of the Programmatic Data Publishing (PDP) service.

The pdp1 convention differs from pdp0 in that it supports providing data files to SIPs.  It 
relies on a shared filesystem mounted on both the client and server machines to facilitate the 
importation of data.  It is intended for use by the MIDAS DAP client.  
"""
import logging, json
from collections import OrderedDict
from typing import Callable, Mapping
from urllib.parse import parse_qs

from .. import PDPublishingService, PDP0Service, PDP1Service, status, UploadMethodNotSupported
from ... import (PublishingStateException, SIPNotFoundError, BadSIPInputError, NERDError,
                 SIPStateException, SIPConflictError, UnauthorizedPublishingRequest)
from .base import PDPHandler
from nistoar.web.rest import ServiceApp
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.pdr.utils.webrecord import WebRecorder
from nistoar.web.utils import order_accepts
from nistoar.web.rest import Handler
from nistoar.nerdm.validate import ValidationError
from nistoar.pdr.preserve.service import PreservationService

class PDPApp(ServiceApp):
    """
    The WSGI SubApp that handles the pdp0 and pdp1 conventions of the PDP service.
    """

    def __init__(self, service: PDPublishingService, parentlog: logging.Logger, config: Mapping={}):
        super(PDPApp, self).__init__(service.convention, parentlog.getChild(service.convention), config)
        self.svc = service

    def sips_for(self, who: Agent):
        """
        return the list of SIPs that the given Agent is managing.  This is determined based on the 
        Agent's ``agent_class`` property.
        """
        if who.agent_class is None:
            return []
        return status.SIPStatus.requests(self.svc.statusdir, who.agent_class)
        
    def create_handler(self, env: dict, start_resp: Callable, path: str, who: Agent) -> Handler:
        """
        return a handler instance to handle a particular request to a path
        :param Mapping env:  the WSGI environment containing the request
        :param Callable start_resp:  the start_resp function to use initiate the response
        :param str path:     the path to the resource being requested.  This is usually 
                             relative to a parent path that this SubApp is configured to 
                             handle.  
        """
        return self._Handler(self, path, env, start_resp, who)

    class _Handler(PDPHandler):
        default_agent = None

        def __init__(self, app, path: str, wsgienv: dict, start_resp: Callable, who=None, config: dict={}):
            Handler.__init__(self, path, wsgienv, start_resp, who, config, app.log, app)
            self._reqrec = None
            if self._app._recorder:
                self._reqrec = self._app._recorder.from_wsgi(self._env)

        def send_error_resp(self, code, reason, explain, sipid=None, pdrid=None, ashead=False):
            """
            respond to client with a JSON-formated error response.
            :param int code:    the HTTP code to respond with 
            :param str reason:  the reason to return as the HTTP status message
            :param str explain: the more extensive explanation as to the reason for the error; 
                                this is returned only in the body of the message
            :param str sipid:   the SIP ID for the requested SIP; if None, it is not applicable or known
            :param str pdrid:   the PDR ID for the requested SIP; if None, it is not applicable or known
            :param bool ashead: if true, do not send the body as this is a HEAD request
            """
            resp = {
                'http:code': code,
                'http:reason': reason,
                'pdr:message': explain,
            }
            if sipid:
                resp['pdr:sipid'] = sipid
            if pdrid:
                resp['@id'] = pdrid

            return self.send_json(resp, reason, code, ashead)

        def do_GET(self, path, ashead=False):
            if not self.acceptable():
                return self.send_unacceptable()

            path = path.lstrip('/')
            if not path:
                return self.send_json(self._app.sips_for(self.who), ashead=ashead)

            parts = path.split('/', 1)
            try:
                # need to check authorization
                stat = self._app.svc.status_of(parts[0])
                if not stat.any_authorized(self.who.agent_class) or stat.state == status.NOT_FOUND:
                    return self.send_error_resp(404, "Authorized SIP Not Found",
                                "There are no SIP submissions viewable for the client's authorization.")

                if len(parts) > 1 and parts[1] == ":status":
                    # just want status information
                    return self.send_json(stat.user_export(), ashead=ashead)

                if len(parts) > 1 and parts[1] == ":data":
                    # request for current data uploads directory
                    if not getattr(self._app.svc, 'uplparent', None):
                        return self.send_error_resp(403, "Data uploads not supported",
                                                    "Data uploads are not allowed on this SIP: "+parts[0])
                    datasrc = self._app.svc.get_upload_space(parts[0], self.who)
                    if not datasrc:
                        return self.send_error_resp(404, "No data sources set",
                                                    "A data uploads directory has not yet been "
                                                    "initialized")
                    datasrc['pdr:sipid'] = parts[0]
                    datasrc['pdr:status'] = stat.state   # pdr:status is deprecated
                    datasrc['pdr:state'] = stat.state
                    return self.send_json(datasrc, ashead=ashead)

                else:
                    # request for nerdm metadata
                    out = self._app.svc.describe(path)
                    out['pdr:status'] = stat.state
                    if out.get('pdr:message') is not None:
                        out['pdr:message'] = status.user_message[stat.state]
                    return self.send_json(out, ashead=ashead)

            except SIPNotFoundError as ex:
                return self.send_error_resp(404, "Not Found", "Requested SIP not found", parts[0])

            except Exception as ex:
                self.log.exception("Failed to describe SIP: %s: %s", parts[0], str(ex))
                return self.send_error(500, "Server error")

        def do_POST(self, path):
            if not self.acceptable():
                return self.send_unacceptable()

            path = path.lstrip('/')

            sipid = ''
            stat = None
            if len(path.split('/', 1)) > 1:
                # cannot POST to paths that appear to point to SIP components
                return self.send_error_resp(405, "Method not allowed on this resource",
                                            "POST method not allowed on this resource")
            if path:
                sipid = path
                stat = self._app.svc.status_of(path)
                if stat.state == status.NOT_FOUND:
                    # SIP must first be created before a component can be added
                    return self.send_error_resp(404, "SIP not found",
                                                "SIP submission not found", sipid)

            action = self.get_action()

            try:
                bodyin = self._env.get('wsgi.input')
                if bodyin is None:
                    if self._reqrec:
                        self._reqrec.record()
                    return self.send_error_resp(400, "Missing input NERDm document",
                                                "No input NERDm document provided to POST", sipid)
                if self.log.isEnabledFor(logging.DEBUG) or self._reqrec:
                    body = bodyin.read()
                    nerdm = json.loads(body, object_pairs_hook=OrderedDict)
                else:
                    nerdm = json.load(bodyin, object_pairs_hook=OrderedDict)
                if self._reqrec:
                    self._reqrec.add_body_text(json.dumps(nerdm, indent=2)).record()

            except (ValueError, TypeError) as ex:
                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.error("Failed to parse input: %s", str(ex))
                    self.log.debug("\n%s", body)
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                return self.send_error_resp(400, "Input not parseable as JSON",
                                            "Input document is not parse-able as JSON: "+str(ex), sipid)

            except Exception as ex:
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                raise

            try:
                success = 200
                if sipid:
                    if not stat.any_authorized(self.who.agent_class):
                        self.info("Agent %s is not authorized to update SIP, %s", self.who.id, sipid)
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
                    out['pdr:message'] = status.user_message[stat.state]
                return self.send_json(out, code=success)

            except NERDError as ex:
                self.log.error("Bad NERDm data POSTed to %s: %s", path, str(ex))
                return self.send_error_resp(400, "Bad Input NERDm data", str(ex), sipid)

            except ValidationError as ex:
                self.log.error("Invalid NERDm data POSTed to %s: %s", path, str(ex))
                return self.send_error_resp(400, "Bad Input NERDm data", str(ex), sipid)

            except SIPNotFoundError as ex:
                return self.send_error_resp(404, "Not Found", "Requested SIP not found", parts[0])

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized create request to %s: %s", path, str(ex))
                return self.send_unauthorized()

            except BadSIPInputError as ex:
                msg = "Bad Input: "+str(ex)
                self.log.error(msg)
                return self.send_error_resp(400, "Bad Input", msg, sipid)

            except PublishingStateException as ex:
                msg = "Attempt to update SIP in un-update-able state: %s" % str(ex)
                self.log.error(msg)
                return self.send_error_resp(409, "Conflicting SIP state", msg, sipid)

            except Exception as ex:
                if sipid:
                    self.log.exception("Failed to accept SIP: %s: %s", sipid, str(ex))
                else:
                    self.log.exception("Failed to accept SIP: %s", str(ex))
                return self.send_error(500, "Server error")
                
        def do_PUT(self, path):
            if not self.acceptable():
                return self.send_unacceptable()

            path = path.lstrip('/')

            if not path:
                return self.send_error_resp(405, "Method not allowed", "PUT not allowed on this resource")

            parts = path.split('/', 1)
            sipid = parts[0]
            compid = None
            if len(parts) > 1:
                compid = parts[1]

            stat = self._app.svc.status_of(sipid)
            if stat.state != status.NOT_FOUND and not stat.any_authorized(self.who.agent_vehicles()):
                self.log.info("%s is not authorized to update SIP, %s", self.who.actor, sipid)
                return self.send_unauthorized()

            action = self.get_action()

            try:
                bodyin = self._env.get('wsgi.input')
                if bodyin is None and compid != ":data":
                    if self._reqrec:
                        self._reqrec.record()
                    return self.send_error_resp(400, "Missing input NERDm document",
                                                "No input NERDm document provided to PUT", sipid)
                elif bodyin:
                    if compid == ':data' or self.log.isEnabledFor(logging.DEBUG) or self._reqrec:
                        body = bodyin.read()
                        nerdm = json.loads(body, object_pairs_hook=OrderedDict) if body else {}
                    else:
                        nerdm = json.load(bodyin, object_pairs_hook=OrderedDict)
                    if self._reqrec:
                        self._reqrec.add_body_text(json.dumps(nerdm, indent=2)).record()

            except (ValueError, TypeError) as ex:
                if self.log.isEnabledFor(logging.DEBUG):
                    self.log.error("Failed to parse input: %s", str(ex))
                    self.log.debug("\n%s", body)
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                return self.send_error_resp(400, "Input not parseable as JSON",
                                            "Input document is not parse-able as JSON: "+str(ex), sipid)

            except Exception as ex:
                if self._reqrec:
                    self._reqrec.add_body_text(body).record()
                raise

            try:
                if compid == ":data":
                    # requesting space for uploads; nerdm is the data source details
                    if nerdm and nerdm.get('type','') != 'fs':
                        return self.send_error_resp(400, "Bad uploads request",
                                                    "Data uploads request only supports type='fs'")
                    out = self._app.svc.init_data_upload(sipid, 'fs', self.who)
                    out['pdr:sipid'] = sipid
                    if action and action != self.ACTION_UPDATE:
                        out['pdr:message'] = f"Note: action={action} ignored on :data endpoint"
                    
                elif compid:
                    # adding component metadata
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
                                                           stat.state == status.NOT_FOUND or
                                                           stat.state == status.PUBLISHED   )

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
                    out['pdr:message'] = status.user_message[stat.state]
                return self.send_json(out)

            except NERDError as ex:
                self.log.error("Bad NERDm data PUT to %s: %s", path, str(ex))
                return self.send_error_resp(400, "Bad Input NERDm data", str(ex), sipid)

            except ValidationError as ex:
                self.log.error("Invalid NERDm data PUT to %s: %s", path, str(ex))
                return self.send_error_resp(400, "Bad Input NERDm data", str(ex), sipid)

            except SIPNotFoundError as ex:
                return self.send_error_resp(404, "Not Found", "Requested SIP not found", parts[0])

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized update request to %s: %s", path, str(ex))
                return self.send_unauthorized()

            except UploadMethodNotSupported as ex:
                msg = "Data uploads not supported on this API"
                self.log.error(msg)
                return self.send_error_resp(400, "Unsupported uploads method", msg, sipid)

            except BadSIPInputError as ex:
                msg = "Bad Input: "+str(ex)
                self.log.error(msg)
                return self.send_error_resp(400, "Bad Input", msg, sipid)

            except PublishingStateException as ex:
                msg = "Attempt to update SIP in un-update-able state: %s" % str(ex)
                self.log.error(msg)
                return self.send_error_resp(409, "Conflicting SIP state", msg, sipid)

            except Exception as ex:
                self.log.exception("Failed to accept SIP update: %s: %s", path, str(ex))
                return self.send_error(500, "Server error")

        def do_DELETE(self, path):
            if not self.acceptable():
                return self.send_unacceptable()

            path = path.lstrip('/')

            if not path:
                return self.send_error_resp(405, "Method not allowed on this resource",
                                            "DELETE method not allowed on this resource")

            parts = path.split('/', 1)
            sipid = parts[0]
            compid = None
            if len(parts) > 1:
                compid = parts[1]

            stat = self._app.svc.status_of(sipid)
            if stat.state == status.NOT_FOUND:
                return self.send_error_resp(404, "SIP not found",
                                            "Unable to DELETE: SIP submission not found", sipid)

            if not stat.any_authorized(self.who.agent_vehicles()):
                self.info("%s is not authorized to update SIP, %s", self.who.actor, sipid)
                return self.send_unauthorized()

            try:
                if compid == ":data":
                    # forget about uploads space
                    try:
                        self._app.svc.cancel_upload_space(sipid, self.who)
                    except SIPConflictError as ex:
                        self.log.error("%s: unable to remove component, %s: %s", sipid, compid, str(ex))
                        return self.send_error_resp(409, "Not in DELETEable state",
                                                    "SIP is not in a DELETEable state: status="+
                                                    status.user_message[stat.state], sipid)
                    
                if compid:
                    try:

                        self._app.svc.remove_component(sipid, compid, self.who)

                    except SIPConflictError as ex:
                        self.log.error("%s: unable to remove component, %s: %s", sipid, compid, str(ex))
                        return self.send_error_resp(409, "Not in DELETEable state",
                                                    "SIP is not in a DELETEable state: status="+
                                                    status.user_message[stat.state], sipid)

                else:
                    try:

                        self._app.svc.delete(sipid, self.who)
    
                    except SIPConflictError as ex:
                        self.log.error("%s: unable to delete SIP: %s", sipid, compid, str(ex))
                        return self.send_error_resp(409, "Not in DELETEable state",
                                                    "SIP is not in a DELETEable state: status="+
                                                    status.user_message[stat.state], sipid)

            except SIPNotFoundError as ex:
                return self.send_error_resp(404, "SIP not found",
                                            "SIP submission not found", sipid)

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized request to delete %s: %s", path, str(ex))
                return self.send_unauthorized()

            except Exception as ex:
                self.log.exception("Failed to DELETE %s: %s", path, str(ex))
                return self.send_error(500, "Server error")

            return self.send_ok()
            
        def do_PATCH(self, path):
            if not self.acceptable():
                return self.send_unacceptable()

            # This method is only used to finalize or publish
            path = path.lstrip('/')

            parts = path.split('/')
            if not path or len(parts) > 1:
                return self.send_error_resp(405, "Method not allowed on this resource",
                                            "PATCH method not allowed on this resource")
            sipid = parts[0]

            action = self.get_action()

            try:
                bodyin = self._env.get('wsgi.input')
                if bodyin:
                   body = bodyin.read(1)
                   if len(body) != 0:
                       return self.send_error_resp(400, "Body not allowed", 
                         "PATCH request should not include a body--only the action query parameter", sipid)

                out = None
                if action == self.ACTION_FINALIZE or action == self.ACTION_PUBLISH:
                    self._app.svc.finalize(sipid, self.who)
                    out = self._app.svc.describe(sipid)

                    if action == self.ACTION_PUBLISH:
                        self._app.svc.publish(sipid, self.who)

                elif action == self.ACTION_IMPORTDATA:
                    imported = self._app.svc.import_files(sipid, self.who)
                    out = self._app.svc.describe(sipid)
                    out['pdr:message'] = f"Imported {len(imported)} file" + \
                        "s" if len(imported) > 0 else ""
                    out['pdr:imported'] = imported

                if not out:
                    out = self._app.svc.describe(sipid)

                stat = self._app.svc.status_of(sipid)
                out['pdr:status'] = stat.state
                out['pdr:state'] = stat.state
                out['pdr:pub_status'] = stat.user_export()
                if out.get('pdr:message') is not None:
                    out['pdr:message'] = status.user_message[stat.state]
                return self.send_json(out)

            except SIPNotFoundError as ex:
                return self.send_error_resp(404, "SIP not found",
                                            "Unable to act on SIP: SIP submission not found", sipid)

            except UnauthorizedPublishingRequest as ex:
                self.log.warning("Unauthorized %s request on %s: %s", action, path, str(ex))
                return self.send_unauthorized()

            except BadSIPInputError as ex:
                msg = "Bad Input: "+str(ex)
                self.log.error(msg)
                return self.send_error_resp(400, "Bad Input", msg, sipid)

            except Exception as ex:
                self.log.exception("Failed to take %s action on %s: %s", action, path, str(ex))
                return self.send_error(500, "Server error")


class PDP0App(PDPApp):
    """
    The WSGI SubApp that handles the pdp0 convention of the PDP service.
    """

    def __init__(self, parentlog: logging.Logger, config: Mapping={}, pressvc: PreservationService=None):
        convention = config.get('convention', 'pdp0')
        pdpsvc = PDP0Service(config, convention, parentlog, pressvc=pressvc)
        super(PDP0App, self).__init__(pdpsvc, parentlog.getChild(convention), config)

class PDP1App(PDPApp):
    """
    The WSGI SubApp that handles the pdp0 convention of the PDP service.
    """

    def __init__(self, parentlog: logging.Logger, config: Mapping={}, pressvc: PreservationService=None):
        convention = config.get('convention', 'pdp1')
        pdpsvc = PDP1Service(config, parentlog, pressvc=pressvc, convention=convention) 
        super(PDP1App, self).__init__(pdpsvc, parentlog.getChild(convention), config)



            
