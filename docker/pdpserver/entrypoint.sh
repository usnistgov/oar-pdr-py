#! /bin/bash
#
port=9090
script=scripts/pdp-uwsgi.py

[ -n "$OAR_WORKING_DIR" ] || OAR_WORKING_DIR=`mktemp -d _pdpserver.XXXXX`
[ -d "$OAR_WORKING_DIR" ] || {
    echo pdpserver: ${OAR_WORKING_DIR}: working directory does not exist
    exit 10
}
[ -n "$OAR_LOG_DIR" ] || export OAR_LOG_DIR=$OAR_WORKING_DIR
[ -n "$OAR_PDPSERVER_CONFIG" ] || OAR_PDPSERVER_CONFIG=docker/pdpserver/pdr-pdp_config.yml

echo
echo Working Dir: $OAR_WORKING_DIR
echo Access the PDP web service at http://localhost:$port/
echo

uwsgi --plugin python3 --http-socket :9090 --wsgi-file $script --static-map /docs=$PWD/docs \
      --set-ph oar_config_file=$OAR_PDPSERVER_CONFIG \
      --set-ph oar_working_dir=$OAR_WORKING_DIR
