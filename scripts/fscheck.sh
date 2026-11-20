#! /bin/bash
#
prog=`basename $0`
too_full=75   # the percent filled that triggers email
fsdirs="/oar"
platform=oardata.nist.gov
sendto=raymond.plante@nist.gov
subject="Attention: Filesystems getting full on $platform"

function percent_used {
    fsdir=$1
    [ -n "$fsdir" ] || return 1
    pcused=`df $fsdir | awk '!/Used/ { print $5 }' | sed -e 's/%//'`
    echo $pcused
}

function message_body {
    fsdir=$1

    cat <<EOF
Filesystems getting full: $fsdir
Platform: $platform

EOF
    df $1
}

function too_full {
    fsdir=$1

    pcused=`percent_used $fsdir`
    [[ "$pcused" =~ ^[0-9]+$ && "$pcused" -lt "$too_full" ]] || \
        echo $fsdir
}

function check_filesystems {
    fullish_filesystems=
    for fs in "$@"; do
        fullish=`too_full $fs`
        [ -z "$fullish" ] || fullish_filesystems="$fullish_filesystems $fullish"
    done

    [ -z "$fullish_filesystems" ] || {
        message_body $fullish_filesystems | mail -s "$subject" $sendto
    }
}

check_filesystems $fsdirs

    
