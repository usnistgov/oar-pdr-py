"""
the uWSGI script for launching the PDP web service.

This script launches an ID resolver as a web service using uwsgi.  For example, one can 
launch the service with the following command:

  uwsgi --plugin python3 --http-socket :9090 --wsgi-file pdp-uwsgi.py \
        --set-ph oar_config_file=pdp_conf.yml --set-ph oar_working_dir=_test

"""
import os, sys, logging, copy
from copy import deepcopy

try:
    import nistoar
except ImportError:
    oarpath = os.environ.get('OAR_PYTHONPATH')
    if not oarpath and 'OAR_HOME' in os.environ:
        oarpath = os.path.join(os.environ['OAR_HOME'], "lib", "python")
    if oarpath:
        sys.path.insert(0, oarpath)
    import nistoar

from nistoar.pdr import config
from nistoar.pdr.publish.service import wsgi

try:
    import uwsgi
except ImportError:
    # simulate uwsgi for testing purpose
    from nistoar.testing import uwsgi
    uwsgi = uwsgi.load()

def _dec(obj):
    # decode an object if it is not None
    return obj.decode() if isinstance(obj, (bytes, bytearray)) else obj

# determine where the configuration is coming from
confsrc = _dec(uwsgi.opt.get("oar_config_file"))
if confsrc:
    cfg = config.resolve_configuration(confsrc)

elif 'oar_config_service' in uwsgi.opt:
    srvc = config.ConfigService(_dec(uwsgi.opt.get('oar_config_service')),
                                _dec(uwsgi.opt.get('oar_config_env')))
    srvc.wait_until_up(int(_dec(uwsgi.opt.get('oar_config_timeout', 10))),
                       True, sys.stderr)
    cfg = srvc.get(_dec(uwsgi.opt.get('oar_config_appname', 'pdr-pdp')))

elif config.service:
    config.service.wait_until_up(int(os.environ.get('OAR_CONFIG_TIMEOUT', 10)),
                                 True, sys.stderr)
    cfg = config.service.get(os.environ.get('OAR_CONFIG_APP', 'pdr-resolve'))

else:
    raise config.ConfigurationException("resolver: nist-oar configuration not provided")

workdir = _dec(uwsgi.opt.get("oar_working_dir"))
if workdir:
    cfg['working_dir'] = workdir

config.configure_log(config=cfg)

application = wsgi.app(cfg)
logging.info("PDP service ready")
