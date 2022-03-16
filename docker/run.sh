#! /bin/bash
#
# run.sh -- launch a docker container to build or test components from this repository
#
# type "run.sh -h" to see detailed help
#
prog=`basename $0`
execdir=`dirname $0`
[ "$execdir" = "" -o "$execdir" = "." ] && execdir=$PWD
codedir=`(cd $execdir/.. > /dev/null 2>&1; pwd)`
os=`uname`
SED_RE_OPT=r
[ "$os" != "Darwin" ] || SED_RE_OPT=E

function usage {
    cat <<EOF

$prog - launch a docker container to build or test components from this repository

SYNOPSIS
  $prog [-d|--docker-build] [--dist-dir DIR] [CMD ...] 
        [DISTNAME|python...] 
        

ARGS:
  python    apply commands to just the python-based distributions

DISTNAMES:  pdr-py

CMDs:
  build     build the software
  test      build the software and run the unit tests
  install   just install the prerequisites (use with shell)
  pdpserver start the PDP web service
  shell     start a shell in the docker container used to build and test
  testshell start a shell in the docker container after installing the software

OPTIONS
  -d        build the required docker containers first
  -t TESTCL include the TESTCL class of tests when testing; as some classes
            of tests are skipped by default, this parameter provides a means 
            of turning them on.
EOF
}

function wordin {
    word=$1
    shift

    echo "$@" | grep -qsw "$word"
}
function docker_images_built {
    for image in "$@"; do
        (docker images | grep -qs $image) || {
            return 1
        }
    done
    return 0
}

set -e

PKGNAME=oar-pdr-py

distvol=
distdir=
dodockbuild=
cmds=
comptypes=
args=()
dargs=()
pyargs=()
angargs=()
jargs=()
testcl=()
while [ "$1" != "" ]; do
    case "$1" in
        -d|--docker-build)
            dodockbuild=1
            ;;
        --dist-dir)
            shift
            distdir="$1"
            mkdir -p $distdir
            distdir=`(cd $distdir > /dev/null 2>&1; pwd)`
            distvol="-v ${distdir}:/app/dist"
            args=(${args[@]} "--dist-dir=/app/dist")
            ;;
        --dist-dir=*)
            distdir=`echo $1 | sed -e 's/[^=]*=//'`
            mkdir -p $distdir
            distdir=`(cd $distdir > /dev/null 2>&1; pwd)`
            distvol="-v ${distdir}:/app/dist"
            args=(${args[@]} "--dist-dir=/app/dist")
            ;;
        -t|--incl-tests)
            shift
            testcl=(${testcl[@]} $1)
            ;;
        --incl-tests=*)
            testcl=(${testcl[@]} `echo $1 | sed -e 's/[^=]*=//'`)
            ;;
        -h|--help)
            usage
            exit
            ;;
        -*)
            args=(${args[@]} $1)
            ;;
        python)
            comptypes="$comptypes $1"
            ;;
        pdr-py)
            wordin python $comptypes || comptypes="$comptypes python"
            pyargs=(${pyargs[@]} $1)
            ;;
        build|install|test|shell|pdpserver)
            cmds="$cmds $1"
            ;;
        *)
            echo Unsupported command: $1
            false
            ;;
    esac
    shift
done

[ -z "$distvol" ] || dargs=(${dargs[@]} "$distvol")
[ -z "${testcl[@]}" ] || {
    dargs=(${dargs[@]} --env OAR_TEST_INCLUDE=\"${testcl[@]}\")
}

comptypes=`echo $comptypes`
cmds=`echo $cmds`
[ -n "$comptypes" ] || comptypes="python"
[ -n "$cmds" ] || cmds="build"
echo "run.sh: Running docker commands [$cmds] on [$comptypes]"

testopts="--cap-add SYS_ADMIN"
volopt="-v ${codedir}:/dev/oar-pdr-py"

# check to see if we need to build the docker images; this can't detect
# changes requiring re-builds.
# 
if [ -z "$dodockbuild" ]; then
    if wordin python $comptypes; then
        docker_images_built $PKGNAME/pdrpytest || dodockbuild=1
    fi
fi
        
[ -z "$dodockbuild" ] || {
    echo '#' Building missing docker containers...
    $execdir/dockbuild.sh
}

# handle angular building and/or testing.  If shell was requested with
# angular, open the shell in the angular test contatiner (angtest).
# 
# Handle python build and/or test
# 
if wordin python $comptypes; then
    
    if wordin build $cmds; then
        # build = makedist
        echo '+' docker run --rm $volopt "${dargs[@]}" $PKGNAME/pdrpytest makedist \
                        "${args[@]}"  "${pyargs[@]}"
        docker run $ti --rm $volopt "${dargs[@]}" $PKGNAME/pdrpytest makedist \
               "${args[@]}"  "${pyargs[@]}"
    fi

    if wordin test $cmds; then
        # test = testall
        echo '+' docker run --rm $volopt "${dargs[@]}" $PKGNAME/pdrpytest testall \
                        "${args[@]}"  "${pyargs[@]}"
        docker run $ti --rm $volopt "${dargs[@]}" $PKGNAME/pdrpytest testall \
               "${args[@]}"  "${pyargs[@]}"
    fi

    if wordin pdpserver $cmds; then
        echo '+' docker run -ti --rm $volopt "${dargs[@]}" -p 9090:9090 $PKGNAME/pdpserver \
                        "${args[@]}"  "${pyargs[@]}"
        exec docker run -ti --rm $volopt "${dargs[@]}" -p 9090:9090 $PKGNAME/pdpserver \
                        "${args[@]}"  "${pyargs[@]}"
    fi

    if wordin shell $cmds; then
        cmd="testshell"
        if wordin install $cmds; then
            cmd="installshell"
        fi
        echo '+' docker run -ti --rm $volopt "${dargs[@]}" $PKGNAME/pdrpytest $cmd \
                        "${args[@]}"  "${pyargs[@]}"
        exec docker run -ti --rm $volopt "${dargs[@]}" $PKGNAME/pdrpytest $cmd \
                        "${args[@]}"  "${pyargs[@]}"
    fi
fi


