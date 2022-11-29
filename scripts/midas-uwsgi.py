"""
the uWSGI script for launching the MIDAS web service.

This script launches the web service using uwsgi.  For example, one can 
launch the service with the following command:

  uwsgi --plugin python3 --http-socket :9090 --wsgi-file midas-uwsgi.py     \
        --set-ph oar_config_file=midas_conf.yml --set-ph oar_working_dir=_test

The configuration data can be provided to this script via a file (as illustrated above) or it 
can be fetch from a configuration service, depending on the environment (see below).  See the 
documentation for nistoar.midas.dbio.wsgi for the configuration parameters supported by this 
service.

This script also pays attention to the following environment variables:

   OAR_HOME            The directory where the OAR PDR system is installed; this 
                          is used to find the OAR PDR python package, nistoar.
   OAR_PYTHONPATH      The directory containing the PDR python module, nistoar.
                          This overrides what is implied by OAR_HOME.
   OAR_CONFIG_SERVICE  The base URL for the configuration service; this is 
                          overridden by the oar_config_service uwsgi variable. 
   OAR_CONFIG_ENV      The application/component name for the configuration; 
                          this is only used if OAR_CONFIG_SERVICE is used.
   OAR_CONFIG_TIMEOUT  The max number of seconds to wait for the configuration 
                          service to come up (default: 10);
                          this is only used if OAR_CONFIG_SERVICE is used.
   OAR_CONFIG_APP      The name of the component/application to retrieve 
                          configuration data for (default: pdr-resolve);
                          this is only used if OAR_CONFIG_SERVICE is used.
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

from nistoar.base import config
from nistoar.midas.dbio import wsgi, MongoDBClientFactory, InMemoryDBClientFactory, FSBasedDBClientFactory

try:
    import uwsgi
except ImportError:
    # simulate uwsgi for testing purpose
    from nistoar.testing import uwsgi
    uwsgi = uwsgi.load()

def _dec(obj):
    # decode an object if it is not None
    return obj.decode() if isinstance(obj, (bytes, bytearray)) else obj

DEF_MIDAS_DB_TYPE="fsbased"

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

# setup the MIDAS database backend
dbtype = _dec(uwsgi.opt.get("oar_midas_db_type"))
if not dbtype:
    dbtype = cfg.get("dbio", {}).get("factory")
if not dbtype:
    dbtype = DEF_MIDAS_DB_TYPE

if dbtype == "fsbased":
    dbdir = os.path.join(cfg.get('working_dir','.'), "dbfiles")
    if not os.path.exists(dbdir):
        os.mkdir(dbdir)
    factory = FSBasedDBClientFactory(cfg.get("dbio", {}), dbdir)
elif dbtype == "mongo":
    dburl = os.environ.get("OAR_MONGODB_URL")
    if not dburl:
        port = ":%s" % os.environ.get("OAR_MONGODB_PORT", "27017")
        cred = ""
        if os.environ.get("OAR_MONGODB_USER"):
            pasw = os.environ.get("OAR_MONGODB_PASS", os.environ.get("OAR_MONGODB_USER"))
            cred = "%s:%s@" % (os.environ.get("OAR_MONGODB_USER"), pasw)
        dburl = "mongodb://%s%s%s/midas" % (cred, os.environ.get("OAR_MONGODB_HOST", "localhost"), port)
    factory = MongoDBClientFactory(cfg.get("dbio", {}), dburl)
elif dbtype == "inmem":
    factory = InMemoryDBClientFactory(cfg.get("dbio", {}))
else:
    raise RuntimeError("Unsupported database type: "+dbtype)

application = wsgi.app(cfg, factory)
logging.info("MIDAS service ready with "+dbtype+" backend")
