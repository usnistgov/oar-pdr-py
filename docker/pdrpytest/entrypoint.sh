#! /bin/bash
#
mongod_ctl=/usr/local/bin/mongod_ctl.sh
[ "$1" = "" ] && exec /bin/bash

case "$1" in
    shell)
        exec /bin/bash
        ;;
    testall|testshell)
        [ -x $mongo_ctl ] && $mongod_ctl start && sleep 1
        exec /usr/local/bin/gosu developer /usr/local/bin/runtests.sh $@
        ;;
    *)
        exec /usr/local/bin/gosu developer /usr/local/bin/mdtests.sh $@
        ;;
esac

    
    
