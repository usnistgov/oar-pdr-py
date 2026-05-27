"""
the uWSGI script for launching the NPS feedback service

This script launches the web service using uwsgi.  For example, one can 
launch the service with the following command:

  uwsgi --plugin python3 --http-socket :9090 --wsgi-file midas-uwsgi.py     \
        --set-ph oar_config_file=midas_conf.yml --set-ph oar_working_dir=_test

The configuration data can be provided to this script via a file (as illustrated above) or it 
can be fetched from a configuration service, depending on the environment (see below).  See the 
documentation for nistoar.midas.dap.extrev.wsgi for the configuration parameters supported by this 
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
                          configuration data for (default: midas-npsfb);
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

import nistoar.midas
from nistoar.base import config
from nistoar.midas.dbio import MongoDBClientFactory, InMemoryDBClientFactory, FSBasedDBClientFactory
from nistoar.midas.dap.extrev import wsgi

try:
    import uwsgi
except ImportError:
    # simulate uwsgi for testing purpose
    from nistoar.testing import uwsgi
    uwsgi = uwsgi.load()

def _dec(obj):
    # byte-decode an object if it is not None
    return obj.decode() if isinstance(obj, (bytes, bytearray)) else obj

DEF_MIDAS_DB_TYPE="fsbased"

# determine where the configuration is coming from
confsvc = None
confsrc = _dec(uwsgi.opt.get("oar_config_file"))
confto = 10
cfg = None
if confsrc:
    cfg = config.resolve_configuration(confsrc)

elif 'oar_config_service' in uwsgi.opt:
    confsvc = config.ConfigService(_dec(uwsgi.opt.get('oar_config_service')),
                                   _dec(uwsgi.opt.get('oar_config_env')))
    confto = int(_dec(uwsgi.opt.get('oar_config_timeout', 10)))

elif config.service:
    confsvc = config.service
    confto = int(os.environ.get('OAR_CONFIG_TIMEOUT', 10))

if cfg is None:
    if confsvc:
        confsvc.wait_until_up(confto, True, sys.stderr)
        cfg = confsvc.get(_dec(uwsgi.opt.get('oar_config_appname', 
                                         os.environ.get('OAR_CONFIG_APP', 'midas-npsfb'))))
    else:
        raise config.ConfigurationException("npsfeedback: nist-oar configuration not provided")

if isinstance(cfg.get("dbio"), str) or isinstance(cfg.get("dap_service"), str):
    # String values here (instead of dictionaries) mean pull the configuration from this name and
    # merge in the parameters found there.  The value is typically "midas-dbio"; this allows us to
    # ensure that the DAP service we use here has the same essential configuration as the DBIO service
    # (which, in production, is running at the same time).  
    altcfg = None
    if uwsgi.opt.get('oar_midas_config_file'):
        altcfg = config.resolve_configuration(_dec(uwsgi.opt.get('oar_midas_config_file')))
    elif not confsvc:
        raise config.ConfigurationException("dbio/dap_service: unable to fetch reference config data: "
                                            "no config service available")

    if isinstance(cfg.get("dbio"), str):
        if not altcfg:
            altcfg = confsvc.get(cfg['dbio'])
        cfg['dbio'] = altcfg.get('dbio', {})
    if isinstance(cfg.get("dap_service"), str):
        if confsvc and (altcfg is None or cfg.get("dap_service") != cfg.get("dbio")):
            altcfg = confsvc.get(cfg['dap_service'])
        if altcfg.get('dap_service'):
            cfg['dap_service'] = altcfg['dap_service']
        elif altcfg.get('services'):
            # altcfg is using the midas-dbio schema
            dapcfg = altcfg['services'].get('dap', {})
            conv = dapcfg.get('default_convention', 'mds3')
            cfg['dap_service'] = dapcfg.get('conventions', {}).get(conv, {})
            cfg['dap_service']['dbio'] = config.merge_config(cfg['dap_service'].get('dbio', {}),  #primary
                                                             dapcfg.get('dbio', {}))              #default
        else:
            raise config.ConfigurationException("dap_service: Unable to find dap configuration in "+
                                                cfg['dap_service'])

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

dbcfg = cfg.get("dbio", {})
if dbtype == "mongo":
    # ensure the DB URL
    if os.environ.get("OAR_MONGODB_URL"):
        dbcfg['db_url'] = os.environ['OAR_MONGODB_URL']
    if not dbcfg.get('db_url'):
        # Build the DB URL from its pieces with env vars taking precedence over the config
        port = ":%s" % os.environ.get("OAR_MONGODB_PORT", dbcfg.get("port", "27017"))
        user = os.environ.get("OAR_MONGODB_USER", dbcfg.get("user"))
        cred = ""
        if user:
            pasw = os.environ.get("OAR_MONGODB_PASS", dbcfg.get("pw", os.environ.get("OAR_MONGODB_USER")))
            cred = "%s:%s@" % (user, pasw)
        host = os.environ.get("OAR_MONGODB_HOST", dbcfg.get("host", "localhost"))
        dbcfg['db_url'] = "mongodb://%s%s%s/midas" % (cred, host, port)

elif dbtype == "fsbased":
    # determine the DB's root directory
    wdir = cfg.get('working_dir','.')
    if not dbcfg.get('db_root_dir'):
        # use a default under the working directory
        dbcfg['db_root_dir'] = os.path.join(wdir, "dbfiles")
        if not os.path.exists(wdir):
            os.mkdir(wdir)
    elif not os.path.isabs(dbcfg['db_root_dir']):
        # if relative, make it relative to the work directory
        dbcfg['db_root_dir'] = os.path.join(wdir, dbcfg['db_root_dir'])
        if not os.path.exists(wdir):
            os.mkdir(wdir)
        if not os.path.exists(dbcfg['db_root_dir']):
            os.makedirs(dbcfg['db_root_dir'])
    if not os.path.exists(dbcfg['db_root_dir']):
        os.mkdir(dbcfg['db_root_dir'])

elif dbtype == "inmem":
    nscfg = cfg.get("dap_service", {}).get("nerdstorage",{})
    if nscfg:
        nscfg['type'] = "inmem"

else:
    raise RuntimeError("Unsupported database type: "+dbtype)

# uwsgi uses the "application" symbol as the WSGI application object
application = wsgi.app(cfg)

msg = f"NPS Feedback service (v{nistoar.midas.__version__}) ready with {dbtype} backend"
print(msg)
logging.info(msg)
