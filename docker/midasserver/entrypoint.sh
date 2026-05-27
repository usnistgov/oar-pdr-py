#! /bin/bash
#
port=9091
npsport=9092
script=/dev/oar-pdr-py/scripts/midas-uwsgi.py
[ -f "$script" ] || script=/app/dist/pdr/bin/midas-uwsgi.py
npsfbscript=/dev/oar-pdr-py/scripts/npsfeedback-uwsgi.py
[ -f "$npsfbscript" ] || npsfbscript=/app/dist/pdr/bin/npsfeedback-uwsgi.py
midas_config=/app/midas-config.yml
npsfb_config=/app/npsfb-config.yml
clinotif_config=/app/midas-clinotif.yml
clinotif_key=123456_secret_key
clinotif_server="python3 /app/dist/pdr/bin/websocket_server.py"

[ -n "$OAR_WORKING_DIR" ] || OAR_WORKING_DIR=`mktemp --tmpdir -d _midasserver.XXXXX`
[ -d "$OAR_WORKING_DIR" ] || {
    echo midasserver: ${OAR_WORKING_DIR}: working directory does not exist
    exit 10
}
[ -n "$OAR_LOG_DIR" ] || export OAR_LOG_DIR=$OAR_WORKING_DIR
[ -n "$OAR_MIDASSERVER_CONFIG" ] || OAR_MIDASSERVER_CONFIG=$midas_config
[ -n "$OAR_NPSFBSERVICE_CONFIG" -o \! -f "$npsfb_config" ] || \
    OAR_NPSFBSERVICE_CONFIG=$npsfb_config
echo npsfb_config=$npsfb_config
ls $npsfb_config
echo OAR_NPSFBSERVICE_CONFIG=$OAR_NPSFBSERVICE_CONFIG
ls $OAR_NPSFBSERVICE_CONFIG

echo
echo Working Dir: $OAR_WORKING_DIR
echo Access the MIDAS web services at http://localhost:$port/
echo

opts=
oar_midas_db_type=$1
[ -z "$oar_midas_db_type" ] || opts="--set-ph oar_midas_db_type=$oar_midas_db_type"
[ -z "$OAR_LOG_FILE" ] || opts="$opts --set-ph oar_log_file=$OAR_LOG_FILE"

# use a client notification server?
use_clinotif_config=
[ -z "$OAR_CLINOTIF_URL" ] || {
    use_clinotif_config=/tmp/client_notifier_config.yml
    touch $use_clinotif_config
    [ \! -f "$clinotif_config" ] || cp $clinotif_config $use_clinotif_config
    if { echo $OAR_CLINOTIF_URL | grep -qs '^ws:'; }; then
        # an external server is running
        echo service_endpoint: $OAR_CLINOTIF_URL >> $use_clinotif_config
        [ -n "$OAR_CLINOTIF_KEY" ] || OAR_CLINOTIF_KEY=$clinotif_key
        echo broadcast_key: $OAR_CLINOTIF_KEY >> $use_clinotif_config
    else
        # we're launching our own
        echo service_endpoint: ws://localhost:8765 >> $use_clinotif_config
        echo port: 8765 >> $use_clinotif_config
        broadkey=`python3 -c 'import uuid; print(uuid.uuid4())'`
        echo broadcast_key: $broadkey >> $use_clinotif_config

        # launch it
        echo '++' nohup $clinotif_server -l $OAR_LOG_DIR/client_notifier.log --config $use_clinotif_config
        nohup $clinotif_server -l $OAR_LOG_DIR/client_notifier.log --config $use_clinotif_config 2>&1 >> $OAR_LOG_DIR/nohup.log &
    fi
}
[ -z "$use_clinotif_config" ] || \
    opts="$opts --set-ph dbio_clinotif_config_file=$use_clinotif_config"

# launch an NPS feedback service?
[ -z "$OAR_NPSFBSERVICE_CONFIG" ] || {
    echo '++' uwsgi --plugin python3 --http-socket :$npsport --wsgi-file $npsfbscript \
                    --set-ph oar_config_file=$OAR_NPSFBSERVICE_CONFIG \
                    --set-ph oar_midas_config_file=$OAR_MIDASSERVER_CONFIG \
                    --set-ph oar_working_dir=$OAR_WORKING_DIR \
                    --daemonize=$OAR_LOG_DIR/npsfb-uwsgi.log
    uwsgi --plugin python3 --http-socket :$npsport --wsgi-file $npsfbscript \
          --set-ph oar_config_file=$OAR_NPSFBSERVICE_CONFIG \
          --set-ph oar_midas_config_file=$OAR_MIDASSERVER_CONFIG \
          --set-ph oar_working_dir=$OAR_WORKING_DIR \
          --daemonize=$OAR_LOG_DIR/npsfb-uwsgi.log >> $OAR_LOG_DIR/nohup.log 
}


echo '++' uwsgi --plugin python3 --http-socket :$port --wsgi-file $script --static-map /docs=/docs \
                --set-ph oar_config_file=$OAR_MIDASSERVER_CONFIG \
                --set-ph oar_working_dir=$OAR_WORKING_DIR $opts
uwsgi --plugin python3 --http-socket :$port --wsgi-file $script --static-map /docs=/docs \
      --set-ph oar_config_file=$OAR_MIDASSERVER_CONFIG \
      --set-ph oar_working_dir=$OAR_WORKING_DIR $opts
