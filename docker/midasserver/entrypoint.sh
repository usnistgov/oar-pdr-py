#! /bin/bash
#
port=9091
script=/dev/oar-pdr-py/scripts/midas-uwsgi.py
[ -f "$script" ] || script=/app/dist/pdr/bin/midas-uwsgi.py

[ -n "$OAR_WORKING_DIR" ] || OAR_WORKING_DIR=`mktemp --tmpdir -d _midasserver.XXXXX`
[ -d "$OAR_WORKING_DIR" ] || {
    echo midasserver: ${OAR_WORKING_DIR}: working directory does not exist
    exit 10
}
[ -n "$OAR_LOG_DIR" ] || export OAR_LOG_DIR=$OAR_WORKING_DIR
[ -n "$OAR_MIDASSERVER_CONFIG" ] || OAR_MIDASSERVER_CONFIG=/app/midas-config.yml

echo
echo Working Dir: $OAR_WORKING_DIR
echo Access the MIDAS web services at http://localhost:$port/
echo

opts=
oar_midas_db_type=$1
[ -z "$oar_midas_db_type" ] || opts="--set-ph oar_midas_db_type=$oar_midas_db_type"

uwsgi --plugin python3 --http-socket :$port --wsgi-file $script --static-map /docs=$PWD/docs \
      --set-ph oar_config_file=$OAR_MIDASSERVER_CONFIG \
      --set-ph oar_working_dir=$OAR_WORKING_DIR $opts
