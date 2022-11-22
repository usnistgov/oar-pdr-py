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
DEFAULT_CONFIGFILE=$dockerdir/midasserver/midas-dmp_conf.yml

set -e

function usage {
    cat <<EOF
$prog - launch a docker container running the midas web server

SYNOPSIS
  $prog [-b|--build] [-D|--docker-build] [-c|--config-file FILE] [-M|--use-mongodb] [DIR] 

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

DOPYBUILD=
DODOCKBUILD=
CONFIGFILE=
USEMONGO=
STOREDIR=
DBTYPE=
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
        -M|--use-mongo)
            DBTYPE="mongo"
            ;;
        -*)
            echo "${prog}: unsupported option:" $1
            false
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

([ -z "$DOPYBUILD" ] && [ -e "$repodir/dist" ]) || {
    echo '+' scripts/install.sh --prefix=$repodir/dist/pdr
    $repodir/scripts/install.sh --prefix=$repodir/dist/pdr
}
[ -d "$repodir/dist/pdr/lib/python/nistoar" ] || {
    echo ${prog}: Python library not found in dist directory: $repodir/dist
    false
}
VOLOPTS="-v $repodir/dist:/app/dist"

# build the docker images if necessary
(docker_images_built midasserver && [ -z "$DODOCKBUILD" ]) || build_server_image

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
VOLOPTS="$VOLOPTS -v ${CONFIGFILE}:/app/midas-config.${configext}:ro"
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

STOP_MONGO=true
if [ "$DBTYPE" = "mongo" ]; then
    DOCKER_COMPOSE="docker compose"
    (docker compose version > /dev/null 2>&1) || DOCKER_COMPOSE=docker-compose
    ($DOCKER_COMPOSE version  > /dev/null 2>&1) || {
        echo ${prog}: docker compose required for -M
        false
    }

    
    dc_vol_file=`mktemp --tmpdir --suffix=.yml docker-compose.volumes.XXXXXX`
    cat > $dc_vol_file <<EOF
version: "3"
volumes: 
  mongo_data:
EOF
    [ -z "$STOREDIR" ] || {
        sdir=`(cd $STOREDIR; pwd)`
        echo "    type: bind"    >> $dc_vol_file
        echo "    source: $sdir" >> $dc_vol_file
    }

    # now launch the database in its own containers
    echo '+' $DOCKER_COMPOSE -f $dockerdir/mongo/docker-compose.mongo.yml -f $dc_vol_file up -d
    $DOCKER_COMPOSE -f $dockerdir/mongo/docker-compose.mongo.yml -f $dc_vol_file up -d

    function stop_mongo {
        $DOCKER_COMPOSE -f $dockerdir/mongo/docker-compose.mongo.yml -f $dc_vol_file
        [ -f "$dc_vol_file" ] || rm $dc_vol_file;
    }
    STOP_MONGO=stop_mongo

    echo 
    echo NOTE:  Visit http://localhost:8081/ to view MongoDB contents
    echo 
fi

CONTAINER_NAME="midasserver"
function stop_server {
    echo '+' docker kill $CONTAINER_NAME
    docker kill $CONTAINER_NAME
}
trap "{ stop_server; $STOP_MONGO; }" EXIT TERM STOP

echo '+' docker run $ENVOPTS $VOLOPTS -p 127.0.0.1:9091:9091/tcp --rm --name=$CONTAINER_NAME $PACKAGE_NAME/midasserver $DBTYPE
docker run $ENVOPTS $VOLOPTS -p 127.0.0.1:9091:9091/tcp --rm --name=$CONTAINER_NAME $PACKAGE_NAME/midasserver $DBTYPE


[ "$DBTYPE" != "mongo" ] || {
    $DOCKER_COMPOSE -f $dockerdir/mongo/docker-compose.mongo.yml -f $dc_vol_file down
    [ ! -f "$dc_vol_file" ] || rm $dc_vol_file
}

