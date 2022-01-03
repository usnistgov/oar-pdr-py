#! /bin/bash
#
#  test-resolver.sh -- launch a resolver service and send it some test requests
#
#
set -e
prog=`basename $0`
execdir=`dirname $0`
[ "$execdir" != "." -a "$execdir" != "" ] || execdir=$PWD

function help {
    echo ${prog} -- launch a pubserver server and send it to some test requests
    cat <<EOF

Usage: $prog [OPTION ...]

Options:
   --deep | -D              only run all available tests, including deep tests that require other 
                            services to running
   --change-dir | -C DIR    change into DIR before launch service.  All path 
                            option arguments are by default relative to this dir
ectory.
   --oar-home | -H DIR      assume DIR to be where the OAR system is installed. 
 
   --config-file | -c FILE  use contents of FILE as the configuration data for the server
   --working-dir | -w DIR   write output files to this directory; if it doesn't exist it 
                            will be created.
   --pid-file | -p FILE     use this file as the server's PID file
   --port | -P #            the port number to run the service on (default: 9090)
   --quiet | -q             suppress most status messages 
   --verbose | -v           print extra messages about internals
   --help | -h              print this help message
EOF
}

quiet=
verbose=
noclean=
withmdserver=
while [ "$1" != "" ]; do
  case "$1" in
      --change-dir|-C)
          [ $# -lt 2 ] && { echo Missing argument to $1 option; false; }
          shift
          echo "${prog}: changing working directory to $1" 1>&2
          cd $1
          ;;
      --oar-home|-H)
          [ $# -lt 2 ] && { echo Missing argument to $1 option; false; }
          shift
          export OAR_HOME=$1
          ;;
      --config-file|-c)
          [ $# -lt 2 ] && { echo Missing argument to $1 option; false; }
          shift
          server_config=$1
          ;;
      --pid-file|-p)
          [ $# -lt 2 ] && { echo Missing argument to $1 option; false; }
          shift
          server_pid_file=$1
          ;;
      --port|-P)
          [ $# -lt 2 ] && { echo Missing argument to $1 option; false; }
          shift
          port=$1
          ;;
      --working-dir|-w)
          [ $# -lt 2 ] && { echo Missing argument to $1 option; false; }
          shift
          workdir=$1
          noclean=1
          ;;
      --deep|-D)
          deep=1
          ;;
      --quiet|-q)
          quiet=1
          ;;
      --verbose|-v)
          verbose=1
          ;;
      --no-clean)
          noclean=1
          ;;
      --help|-h)
          help
          exit
          ;;
      --*)
          echo ${prog}: unsupported option: $1 1>&2
          false
          ;;
      *)
          pods=("${pods[@]}" "$1")
          ;;
  esac
  shift
done

[ -n "$workdir" ] || {
    workdir=`echo $prog | sed -e 's/\.[bash]+//'`
    workdir="_${workdir}-$$"
}
[ -d "$workdir" ] || mkdir $workdir

[ -n "$server_config" ] || {
    cat > $workdir/service-conf.yml <<EOF
locations:
   metadataService:     https://data.nist.gov/rmm
   landingPageService:  https://data.nist.gov/pdr/lps
   distributionService: https://data.nist.gov/od/ds
EOF
    server_config=$workdir/service-conf.yml
}

[ -n "$uwsgi_script" ] || {
    if [ -f scripts/resolver-uwsgi.py ]; then
        uwsgi_script=scripts/resolver-uwsgi.py
    else
        [ -n "$OAR_HOME" ] || {
            echo ${prog}: OAR_HOME not set 1>&2
            false
        }
        uwsgi_script=$OAR_HOME/bin/resolver-uwsgi.py
    fi
}
[ -f "$uwsgi_script" ] || {
    echo ${prog}: server uwsgi file does not exist as file: $uwsgi_script 1>&2
    false
}

[ -n "$server_pid_file" ] || server_pid_file=$workdir/resolver.pid
[ -n "$port" ] || port="9090"
set +e

function tell {
    [ -n "$quiet" ] || echo "$@"
}

function exitopwith { 
    echo $2 > $1.exit
    exit $2
}

function launch_test_server {
    portno=$1
    [ -n "$portno" ] || portno=9090
    tell starting uwsgi for resolver on port $portno...
    [ -n "$quiet" -o -z "$verbose" ] || set -x
    uwsgi --daemonize $workdir/uwsgi.log --plugin python3 \
          --http-socket :$portno --wsgi-file $uwsgi_script --pidfile $server_pid_file \
          --set-ph oar_config_file=$server_config 
    set +x
}

function deorbit_test_server {
    tell stopping uwsgi for resolver...
    uwsgi --stop $server_pid_file
}

function diagnose {
    # spit out some outputs that will help what went wrong with service calls
    # set +x
    [ -z "$1" ] || [ ! -f "$1" ] || {
        echo "============="
        echo Output:
        echo "-------------"
        cat $1
    }
    [ -z "$2" ] || [ ! -f "$2" ] || {
        echo "============="
        echo Log:
        tail "$2"
    }
    # set -x
}

function run_shallow_tests {
    failures=0
    base=$1
    curlcmd=(curl -o/dev/null --silent -w '%{http_code}\n')

    turl="/"
    [ -z "$verbose" ] || echo + "${curlcmd[@]}" $base$turl
    code=`"${curlcmd[@]}" $base$turl`
    [ "$code" == 200 ] || {
        tell "${curlcmd[@]}" $base$turl
        tell "Failed health check"
        ((failures += 1))
    }

    turl="/id/mds2-2199?format=html"
    [ -z "$verbose" ] || echo + "${curlcmd[@]}" $base$turl
    code=`"${curlcmd[@]}" $base$turl`
    [ "$code" == 307 ] || {
        tell "${curlcmd[@]}" $base$turl
        tell "Failed Landing page forwarding"
        ((failures += 1))
    }

    return $failures
}

function run_deep_tests {
    failures=0
    base=$1
    curlcmd=(curl -o/dev/null --silent -w '%{http_code}\n')

    turl="/id/mds2-2199?format=nerdm"
    [ -z "$verbose" ] || echo + "${curlcmd[@]}" $base$turl
    code=`"${curlcmd[@]}" $base$turl`
    [ "$code" == 200 ] || {
        tell "${curlcmd[@]}" $base$turl
        tell "Failed NERDm retrieval"
        ((failures += 1))
    }

    turl="/id/goob?format=nerdm"
    [ -z "$verbose" ] || echo + "${curlcmd[@]}" $base$turl
    code=`"${curlcmd[@]}" $base$turl`
    [ "$code" == 404 ] || {
        tell "${curlcmd[@]}" $base$turl
        tell "Failed NERDm retrieval"
        ((failures += 1))
    }

    return $failures
}

function fatal_shutdown {
    if [ "$0" != "bash" ]; then
        tell "Unexpected failure; shutting down..."
        [ -f "$server_pid_file" ] && deorbit_test_server
    fi
}
trap fatal_shutdown ERR

baseurl="http://localhost:$port"
if [ "$0" != "bash" ]; then

    set -e
    launch_test_server $port

    nf=0
    run_shallow_tests $baseurl || ((nf += $?))
    [ -z "$deep" ] || run_deep_tests $baseurl || ((nf += $?))

    deorbit_test_server
    if [ -z "$noclean" ]; then
        [ -z "$verbose" ] || tell Cleaning up workdir \(`basename $workdir`\)
        rm -rf $workdir
    else
        echo Will not clean-up workdir: $workdir
    fi

    if [ "$nf" = "0" ]; then
        tell "OK: All resolver tests passed."
    else
        tell "NOT OK: Number test failures: $nf"
    fi
    exit $nf

fi
