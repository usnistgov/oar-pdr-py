#! /bin/bash
#
# run.sh -- launch the server in a docker container
#

prog=midasserver
execdir=`dirname $0`
[ "$execdir" = "" -o "$execdir" = "." ] && execdir=$PWD
dockerdir=`(cd $execdir/.. > /dev/null 2>&1; pwd)`
repodir=`(cd $dockerdir/.. > /dev/null 2>&1; pwd)`
scriptsdir=$repodir/scripts
os=`uname`
SED_RE_OPT=r
[ "$os" != "Darwin" ] || SED_RE_OPT=E

PACKAGE_NAME=oar-pdr-py
DEFAULT_CONFIGFILE=$dockerdir/midasserver/midas-dmpdap_conf.yml
NSD_CONFIGFILE=$dockerdir/midasserver/midas-dmpdapnsd_conf.yml

set -e

function usage {
    cat <<EOF
$prog - launch a docker container running the midas web server

SYNOPSIS
  $prog [-h|--help] [-b|--build] [-D|--docker-build] [-c|--config-file FILE] 
              [-M|--use-mongodb] [DIR] [start|stop]

ARGUMENTS
  start                         Start the service; this is the default if the 
                                start|stop argument is not provided.  
  stop                          Stop the running service.  If -M was used to 
                                start the service, it must also provided when
                                stopping it.  
  DIR                           a directory where the database data backing the 
                                server will be stored.  If not provided, a 
                                temporary directory within the midasserver 
                                container will be used.  Provide this if you want
                                look at the database contents directly.
  -b, --build                   Rebuild the python library and install into dist;
                                This is done automatically if the dist directory 
                                does not exist.
  -D, --docker-build            Rebuild the midasserver docker image; this is 
                                done automatically if the midasserver image 
                                does not exist.
  -c FILE, --config-file FILE   Use a custom service configuration given in FILE.
                                This file must be in YAML or JSON format.
                                Defaut: docker/midasserver/midas-dmp_config.yml
  -B, --bg                      Run the server in the background (returning the 
                                command prompt after successful launch)
  -M, --use-mongodb             Use a MongoDB backend; DIR must also be provided.
                                If not set, a file-based database (using JSON 
                                files) will be used, stored under DIR/dbfiles.
  -P, --add-people-service      Include the staff directory service within the application.
                                This will trigger use of a MongoDB database, but it 
                                does not effect the DBIO backend (use -M for this).  
  -p, --port NUM                The port that the service should listen to 
                                (default: 9091)
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
    echo '+' $dockerdir/dockbuild.sh -d midasserver
    $dockerdir/dockbuild.sh -d midasserver # > log
}

PORT=9091
DOPYBUILD=
DODOCKBUILD=
CONFIGFILE=
USEMONGO=
STOREDIR=
DBTYPE=
ADDNSD=
DETACH=
VOLOPTS="-v $repodir/dist:/app/dist"
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
        -p)
            shift
            PORT=$1
            ;;
        --port=*)
            PORT=`echo $1 | sed -e 's/[^=]*=//'`
            ;;
        -B|--bg|--detach)
            DETACH="--detach"
            ;;
        -M|--use-mongo)
            DBTYPE="mongo"
            ;;
        -P|--add-people-service)
            ADDNSD=1
            ;;
        --mount-volume=*)
            vol=`echo $1 | sed -e 's/[^=]*=//'`
            VOLOPTS="$VOLOPTS -v $vol"
            ;;
        -V)
            shift
            vol=$1
            VOLOPTS="$VOLOPTS -v $vol"
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

# build the docker images if necessary
(docker_images_built midasserver && [ -z "$DODOCKBUILD" ]) || build_server_image

[ -n "$CONFIGFILE" ] || {
    CONFIGFILE=$DEFAULT_CONFIGFILE
    [ -z "$ADDNSD" ] || CONFIGFILE=$NSD_CONFIGFILE
}
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
VOLOPTS="$VOLOPTS -v ${configfile}:/app/midas-config.${configext}:ro"
ENVOPTS="-e OAR_MIDASSERVER_CONFIG=/app/midas-config.${configext}"

if [ -d "$repodir/docs" ]; then
    VOLOPTS="$VOLOPTS -v $repodir/docs:/docs"
fi

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
    VOLOPTS="$VOLOPTS -v ${sdir}:/data/midas"
    ENVOPTS="$ENVOPTS -e OAR_WORKING_DIR=/data/midas"
}

NETOPTS=
STOP_MONGO=true
if [ "$DBTYPE" = "mongo" -o -n "$ADDNSD" ]; then
    DOCKER_COMPOSE="docker compose"
    (docker compose version > /dev/null 2>&1) || DOCKER_COMPOSE=docker-compose
    ($DOCKER_COMPOSE version  > /dev/null 2>&1) || {
        echo ${prog}: docker compose required for -M
        false
    }

    echo '+' source $dockerdir/midasserver/mongo/mongo.env
    source $dockerdir/midasserver/mongo/mongo.env

    [ -n "$STOREDIR" -o "$ACTION" = "stop" ] || {
        echo ${prog}: DIR argument must be provided with -M/--use-mongo or -P/--add-people-service
        false
    }
    export OAR_MONGODB_DBDIR=`cd $STOREDIR; pwd`/mongo

    NETOPTS="--network=mongo_default --link midas_mongodb:mongodb"
    ENVOPTS="$ENVOPTS -e OAR_MONGODB_HOST=mongodb -e OAR_MONGODB_USER=oarop"

    [ "$ACTION" = "stop" ] || {
        # now launch the database in its own containers
        echo '+' $DOCKER_COMPOSE -f $dockerdir/midasserver/mongo/docker-compose.mongo.yml up -d
        $DOCKER_COMPOSE -f $dockerdir/midasserver/mongo/docker-compose.mongo.yml up -d

        echo 
        echo NOTE:  Visit http://localhost:8081/ to view MongoDB contents
        echo 
    }

    function stop_mongo {
        echo '+' $DOCKER_COMPOSE -f $dockerdir/midasserver/mongo/docker-compose.mongo.yml down
        $DOCKER_COMPOSE -f $dockerdir/midasserver/mongo/docker-compose.mongo.yml down
    }
    STOP_MONGO=stop_mongo
fi

CONTAINER_NAME="midasserver"
function stop_server {
    echo '+' docker kill $CONTAINER_NAME
    docker kill $CONTAINER_NAME
}
trap "{ stop_server; $STOP_MONGO; }" TERM STOP

if [ "$ACTION" = "stop" ]; then
    echo Shutting down the midas server...
    stop_server || true
    $STOP_MONGO
else
    echo '+' docker run $ENVOPTS $VOLOPTS $NETOPTS -p 127.0.0.1:${PORT}:${PORT}/tcp --rm --name=$CONTAINER_NAME $DETACH $PACKAGE_NAME/midasserver $DBTYPE
    docker run $ENVOPTS $VOLOPTS $NETOPTS -p 127.0.0.1:${PORT}:${PORT}/tcp --rm --name=$CONTAINER_NAME $DETACH $PACKAGE_NAME/midasserver $DBTYPE
fi

