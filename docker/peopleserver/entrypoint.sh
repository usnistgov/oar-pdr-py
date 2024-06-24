#! /bin/bash
#
script=scripts/people-uwsgi.py
port=9092
[ -z "$OAR_PEOPLESERVER_PORT" ] || port=$OAR_PEOPLESERVER_PORT
[ -n "$OAR_WORKING_DIR" ] || OAR_WORKING_DIR=`mktemp --tmpdir -d _nsdserver.XXXXX`
[ -d "$OAR_WORKING_DIR" ] || {
    echo peopleserver: ${OAR_WORKING_DIR}: working directory does not exist
    exit 10
}
[ -n "$OAR_LOG_DIR" ] || export OAR_LOG_DIR=$OAR_WORKING_DIR
[ -n "$OAR_PEOPLESERVER_CONFIG" ] || OAR_PEOPLESERVER_CONFIG=docker/peopleserver/people_config.yml

echo
echo Working Dir: $OAR_WORKING_DIR
echo Access the NSD web service at http://localhost:$port/
echo

uwsgi --plugin python3 --http-socket :$port --wsgi-file $script \
      --set-ph oar_config_file=$OAR_PEOPLESERVER_CONFIG \
      --set-ph oar_working_dir=$OAR_WORKING_DIR
