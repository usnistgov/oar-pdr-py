#! /bin/bash
#
# Processes command line arguments for dockbuild.sh and defines functions it
# can use.
#
set -e
true ${prog:=_dockbuild.sh}

[ -z "$codedir" ] && {
    echo "${prog}: \$codedir is not set."
    exit 10
}

true ${OAR_BUILD_DIR:=$codedir/oar-build}
true ${OAR_DOCKER_DIR:=$codedir/docker}
true ${PACKAGE_NAME:=`basename $codedir`}

[ -n "$DOCKER_IMAGE_DIRS" ] || \
    DOCKER_IMAGE_DIRS=`echo $DEP_DOCKER_IMAGE_DIRS $EXEC_DOCKER_IMAGE_DIRS`
[ -n "$DOCKER_IMAGE_DIRS" ] || {
    for item in `ls $OAR_DOCKER_DIR`; do
        [ -d "$item" -a -f "$item/Dockerfile" ] && \
            DOCKER_IMAGE_DIRS="$DOCKER_IMAGE_DIRS $item"
    done
}

LOGFILE=dockbuild.log
LOGPATH=$PWD/$LOGFILE
. $OAR_BUILD_DIR/_logging.sh

function sort_build_images {
    # Determine which images to build
    #
    # Input:  list of the requested images (on the command line)
    #
    
    if [ "$#" -eq 0 ]; then
        # no images are mentioned on the command line, build them all
        # 
        out=$DOCKER_IMAGE_DIRS

    else
        # make sure we build them in the right order
        #
        imgs=:`echo $@ | tr ' ' :`:
        out=
        for img in $DOCKER_IMAGE_DIRS; do
            (echo $imgs | grep -qs ":${img}:") && \
                out="$out $img"
        done
    fi

    echo $out
}

function index_of_word {
    args=(`echo $@`)
    find=${args[0]}
    words=(${args[@]:1})
    for i in "${!words[@]}"; do
        if [ "${words[$i]}" == "$find" ]; then
            echo $i
            return 0
        fi
    done
}

function word_is_in {
    words=:`echo $@ | sed -e 's/^.* //' -e 's/ /:/'`:
    echo $words | grep -qs :$1:
}

function dependency_images {
    # check for exec image request; include all dependency image if match found
    out=
    for img in $@; do
        i=`index_of_word $img $EXEC_DOCKER_IMAGE_DIRS`
        [ -z "$i" ] || {
            echo $DEP_DOCKER_IMAGE_DIRS
            return 0
        }
    done

    max=
    for im in $@; do
        i=`index_of_word $im $DEP_DOCKER_IMAGE_DIRS`
        [ -z "$i" ] || ([ -n "$max" ] && [ "$i" -le "$max" ]) || max=$i
    done
    deps=($DEP_DOCKER_IMAGE_DIRS)
    [ -z "$max" ] || out="${deps[@]:0:$max}"
    echo $out
}

function get_build_images_with_deps {
    deps=`dependency_images $@`
    out=:`echo $deps | tr ' ' :`:
    for img in $@; do
        (echo $out | grep -sq ":$img:") || out="${out}${img}:"
    done
    echo $out | tr : ' '
}

function collect_build_opts {
    [ -n "$OAR_DOCKER_UID" ] || OAR_DOCKER_UID=`id -u`
    echo "--build-arg=devuid=$OAR_DOCKER_UID"
}

function setup_build {
    if [ -n "$DODEPS" ]; then
        BUILD_IMAGES=`get_build_images_with_deps $do_BUILD_IMAGES`
    else
        BUILD_IMAGES=`sort_build_images $do_BUILD_IMAGES`
    fi
    BUILD_OPTS=`collect_build_opts`
}

function help {
    helpfile=$OAR_BUILD_DIR/dockbuild_help.txt
    [ -f "$OAR_DOCKER_DIR/dockbuild_help.txt" ] && \
        helpfile=$OAR_DOCKER_DIR/dockbuild_help.txt
    sed -e "s/%PROG%/$prog/g" $helpfile
}

CL4LOG=$@

do_BUILD_IMAGES=
while [ "$1" != "" ]; do
    case "$1" in
        --logfile=*)
            LOGPATH=`echo $1 | sed -e 's/[^=]*=//'`
            ;;
        -l)
            shift
            LOGPATH=$1
            ;;
        --build-dependencies|-d)
            DODEPS=-d
            ;;
        --quiet|-q)
            QUIET=-q
            ;;
        --help|-h)
            help
            exit 0
            ;;
        -*)
            echo "${prog}: unsupported option:" $1 "(should this be placed after cmd?)"
            false
            ;;
        *)
            do_BUILD_IMAGES=`echo ${do_BUILD_IMAGES} ${1}`
            ;;
    esac
    shift
done

(echo $LOGPATH | egrep -qs '^/') || LOGPATH=$PWD/$LOGPATH

# Set the user that Docker containers should run as.  Be default, this is set
# to the user running this script so that any files created within the container
# will be owned by this user (rather than, say, root).
#
OAR_DOCKER_UID=`id -u`

# Build from inside the docker dir
# 
cd $OAR_DOCKER_DIR
