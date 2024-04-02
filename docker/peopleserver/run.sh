#! /bin/bash
#
# run.sh -- launch the server in a docker container
#

prog=peopleserver
execdir=`dirname $0`
[ "$execdir" = "" -o "$execdir" = "." ] && execdir=$PWD
dockerdir=`(cd $execdir/.. > /dev/null 2>&1; pwd)`
repodir=`(cd $dockerdir/.. > /dev/null 2>&1; pwd)`
scriptsdir=$repodir/scripts
os=`uname`
SED_RE_OPT=r
[ "$os" != "Darwin" ] || SED_RE_OPT=E

PACKAGE_NAME=oar-pdr-py
DEFAULT_CONFIGFILE=$dockerdir/peopleserver/people_conf.yml

set -e

function usage {
    cat <<EOF
$prog - launch a docker container running the NSD web server

SYNOPSIS
  $prog [-h|--help] [-b|--build] [-D|--docker-build] [-c|--config-file FILE] 
              [DIR] [start|stop]

ARGUMENTS
  start                         Start the service; this is the default if the 
                                start|stop argument is not provided.  
  stop                          Stop the running service.  If -M was used to 
                                start the service, it must also provided when
                                stopping it.  
  DIR                           a directory where the database data backing the 
                                server will be stored.  If not provided, a 
                                temporary directory within the peopleserver 
                                container will be used.  Provide this if you want
                                look at the database contents directly.
  -b, --build                   Rebuild the python library and install into dist;
                                This is done automatically if the dist directory 
                                does not exist.
  -D, --docker-build            Rebuild the peopleserver docker image; this is 
                                done automatically if the peopleserver image 
                                does not exist.
  -c FILE, --config-file FILE   Use a custom service configuration given in FILE.
                                This file must be in YAML or JSON format.
                                Defaut: docker/peopleserver/people_config.yml
  -B, --bg                      Run the server in the background (returning the 
                                command prompt after successful launch)
  -N, --no-mongo                Do not start a mongo server; use this if you want
                                to use a server already running (e.g. as part of 
                                midasserver).
  -p, --port NUM                The port that the service should listen to 
                                (default: 9091)
  -u, --for-unit-tests          print a message indicating the MongoDB URL to use 
                                with for running unit tests
  -h, --help                    Print this text to the terminal and then exit

EOF
}

function docker_images_built {
    for image in "$@"; do
        (docker images | grep -qs $image) || {
            return 1
        }
    done
    return 0
}

function build_server_image {
    echo '+' $dockerdir/dockbuild.sh -d peopleserver
    $dockerdir/dockbuild.sh -d peopleserver # > log
}

PORT=9092
DOPYBUILD=
DODOCKBUILD=
CONFIGFILE=
USEMONGO=1
STOREDIR=
DETACH=
DATADIR=
PRINTURL=
while [ "$1" != "" ]; do
    case "$1" in
        -b|--build)
            DOPYBUILD="-b"
            ;;
        -D|--docker-build)
            DODOCKBUILD="-D"
            ;;
        -c)
            shift
            CONFIGFILE=$1
            ;;
        --config-file=*)
            CONFIGFILE=`echo $1 | sed -e 's/[^=]*=//'`
            ;;
        -B|--bg|--detach)
            DETACH="--detach"
            ;;
        -N|--no-mongo)
            USEMONGO=
            ;;
        -d|--data-dir)
            shift
            DATA_DIR=$1
            ;;
        --data-dir=*)
            DATADIR=`echo $1 | sed -e 's/[^=]*=//'`
            ;;
        -p)
            shift
            PORT=$1
            ;;
        --port=*)
            PORT=`echo $1 | sed -e 's/[^=]*=//'`
            ;;
        -u|--for-unit-tests)
            PRINTURL=1
            ;;
        -h|--help)
            usage
            exit
            ;;
        -*)
            echo "${prog}: unsupported option:" $1
            false
            ;;
        start|stop)
            [ -z "$ACTION" ] || {
                echo "${prog}: Action $ACTION already set; provide only one"
                false
            }
            ACTION=`echo $1 | tr A-Z a-z`
            ;;
        *)
            [ -z "$STOREDIR" ] || {
                echo "${prog}: DIR already set to $STOREDIR; unsupported extra argument:" $1
                false
            }
            STOREDIR=$1
            ;;
    esac
    shift
done
[ -n "$ACTION" ] || ACTION=start

([ -z "$DOPYBUILD" ] && [ -e "$repodir/dist/pdr" ]) || {
    echo '+' scripts/install.sh --prefix=$repodir/dist/pdr
    $repodir/scripts/install.sh --prefix=$repodir/dist/pdr
}
[ -d "$repodir/dist/pdr/lib/python/nistoar" ] || {
    echo ${prog}: Python library not found in dist directory: $repodir/dist
    false
}
VOLOPTS="-v $repodir/dist:/app/dist"
VOLOPTS="$VOLOPTS -v $repodir/scripts/people-uwsgi.py:/dev/oar-pdr-py/scripts/people-uwsgi.py"

[ -n "$DATADIR" ] || DATADIR=$repodir/docker/peopleserver/data
ls $DATADIR/*.json > /dev/null 2>&1 || {
    # no JSON data found in datadir; reset the datadir to test data
    echo "${prog}: no people data found; will load db with test data"
    DATADIR=$repodir/python/tests/nistoar/nsd/data
}
[ "$ACTION" = "stop" ] || echo "${prog}: loading DB from $DATADIR"
VOLOPTS="$VOLOPTS -v ${DATADIR}:/app/data"

# build the docker images if necessary
(docker_images_built peopleserver && [ -z "$DODOCKBUILD" ]) || build_server_image

[ -n "$CONFIGFILE" ] || CONFIGFILE=$DEFAULT_CONFIGFILE
[ -f "$CONFIGFILE" ] || {
    echo "${prog}: Config file ${CONFIGFILE}: does not exist as a file"
    false
}
configext=`echo $CONFIGFILE | sed -e 's/^.*\.//' | tr A-Z a-z`
[ "$configext" = "json" -o "$configext" = "yml" ] || {
    echo "${prog}:" Config file type not recognized by extension: $configext
    false
}
configparent=`dirname $CONFIGFILE`
configfile=`(cd $configparent; pwd)`/`basename $CONFIGFILE`
VOLOPTS="$VOLOPTS -v ${configfile}:/app/people-config.${configext}:ro"
ENVOPTS="-e OAR_PEOPLESERVER_PORT=$PORT -e OAR_PEOPLESERVER_CONFIG=/app/people-config.${configext}"

[ -z "$STOREDIR" ] || {
    [ -d "$STOREDIR" ] || {
        parent=`dirname $STOREDIR`
        if [ -d "$parent" ]; then
            mkdir $STOREDIR
        else
            echo "${prog}: ${STOREDIR}: storage directory not found"
            false
        fi
    }
    sdir=`cd $STOREDIR; pwd`
    VOLOPTS="$VOLOPTS -v ${sdir}:/data/nsd"
    ENVOPTS="$ENVOPTS -e OAR_WORKING_DIR=/data/nsd"
}

NETOPTS=
STOP_MONGO=true

if [ -n "$USEMONGO" ]; then
    DOCKER_COMPOSE="docker compose"
    (docker compose version > /dev/null 2>&1) || DOCKER_COMPOSE=docker-compose
    ($DOCKER_COMPOSE version  > /dev/null 2>&1) || {
        echo ${prog}: docker compose required for -M
        false
    }

    echo '+' source $dockerdir/peopleserver/mongo/mongo.env
    source $dockerdir/peopleserver/mongo/mongo.env

    # [ -n "$STOREDIR" -o "$ACTION" = "stop" ] || {
    #     echo ${prog}: DIR argument must be provided with -M/--use-mongo
    #     false
    # }
    if [ -n "$STOREDIR" ]; then
        export OAR_MONGODB_DBDIR=`cd $STOREDIR; pwd`/mongo
    else
        export OAR_MONGODB_DBDIR=`mktemp --tmpdir -d _nsdmongo.XXXXX`
    fi

    NETOPTS="--network=mongo_default --link mongodb_server:mongodb"
    ENVOPTS="$ENVOPTS -e OAR_MONGODB_HOST=mongodb -e OAR_MONGODB_USER=oarop"

    [ "$ACTION" = "stop" ] || {
        # now launch the database in its own containers
        echo '+' $DOCKER_COMPOSE -f $dockerdir/peopleserver/mongo/docker-compose.mongo.yml up -d
        $DOCKER_COMPOSE -f $dockerdir/peopleserver/mongo/docker-compose.mongo.yml up -d

        echo 
        echo NOTE:  Visit http://localhost:8081/ to view MongoDB contents
        echo 
    }

    function stop_mongo {
        echo '+' $DOCKER_COMPOSE -f $dockerdir/peopleserver/mongo/docker-compose.mongo.yml down
        $DOCKER_COMPOSE -f $dockerdir/peopleserver/mongo/docker-compose.mongo.yml down
    }
    STOP_MONGO=stop_mongo
fi

CONTAINER_NAME="peopleserver"
function stop_server {
    echo '+' docker kill $CONTAINER_NAME
    docker kill $CONTAINER_NAME
}
trap "{ stop_server; $STOP_MONGO; }" TERM STOP

function print_unittest_url {
    echo "Set environment for unit tests: $SHELL"
    if [[ "$SHELL" == *csh ]]; then
        echo "  setenv MONGO_TESTDB_URL 'mongodb://admin:admin@localhost:27017/nsdtest?authSource=admin'"
    else
        echo "  export MONGO_TESTDB_URL='mongodb://admin:admin@localhost:27017/nsdtest?authSource=admin'"
    fi
}

if [ "$ACTION" = "stop" ]; then
    echo Shutting down the NSD server...
    stop_server || true
    $STOP_MONGO
else
    [ -z "$PRINTURL" -o -n "$DETACH" ] || print_unittest_url

    echo '+' docker run $ENVOPTS $VOLOPTS $NETOPTS -p 127.0.0.1:${PORT}:${PORT}/tcp --rm --name=$CONTAINER_NAME $DETACH $PACKAGE_NAME/peopleserver $DBTYPE
    docker run $ENVOPTS $VOLOPTS $NETOPTS -p 127.0.0.1:${PORT}:${PORT}/tcp --rm --name=$CONTAINER_NAME $DETACH $PACKAGE_NAME/peopleserver $DBTYPE

    [ -z "$PRINTURL" -o -z "$DETACH" ] || print_unittest_url
fi

    
