"""
ResolverReady: A top level proof-of-life handler
"""

from nistoar.web.rest import Ready

class ResolverReady(Ready):

    def __init__(self, path, wsgienv, start_resp, config={}, log=None, app=None):
        """
        instantiate the handler
        """
        super(ResolverReady, self).__init__(path, wsgienv, start_resp,
                                            config=config, log=log, app=app)

    def get_ready_html(self, contenttype, ashead=None):
        out = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <title>Resolver Service: Ready</title>
  </head>
  <body>
    <h1>Resolver Service Is Ready</h1>
    <p>
       Available Resolver Endpoints Include:
       <ul>
         <li> <a href="id/">/id/</a> -- for resolving PDR (ARK-based) identifiers </li>
         <li> <a href="aip/">/aip/</a> -- for resolving PDR AIP identifiers </li>
       </ul>
    </p>
  </body>
</html>
"""      
        return self.send_ok(out, contenttype, "Ready", ashead=ashead)
    
