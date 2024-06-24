#! /bin/bash
#
function install {
    scripts/setversion.sh
    scripts/install.sh --prefix=/app/pdr || return 1
    export OAR_HOME=/app/pdr
    export PYTHONPATH=$OAR_HOME/lib/python
    export OAR_LOG_DIR=$OAR_HOME/var/logs
}

function exitopwith { 
    echo $2 > $1.exit
    exit $2
}



cmd=$1
case "$1" in
    makedist)
        shift
        scripts/makedist "$@"
        EXCODE=$?
        ;;
    build)
        scripts/setversion.sh
        (cd python && python setup.py build)
        EXCODE=$?
        ;;
    testall)
        install || {
            echo "testall: Failed to install oar-pdr-py"
            exitopwith testall 2
        }
        shift

        # wrapper root shell should have already started mongodb
        stat=0
        scripts/testall "$@"; stat=$?

        [ "$stat" != "0" ] && {
            echo "testall: One or more test packages failed (last=$stat)"
            echo NOT OK
            exitopwith testall 3
        }
        # echo All OK
        EXCODE=$stat
        ;;
    install)
        install
        python -c 'import nistoar.pdr, jq'
        EXCODE=$?
        ;;
    testshell)
        # wrapper root shell should have already started mongodb
        install
        exec /bin/bash
        ;;
    shell)
        exec /bin/bash
        ;;
    *)
        echo Unknown command: $1
        echo Available commands:  build makedist testall install shell
        EXCODE=100
        ;;
esac

echo $EXCODE > $cmd.exit
exit $EXCODE
