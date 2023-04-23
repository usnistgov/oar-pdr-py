#! /bin/bash
#
# This script installs the PDR python-based system (including metadata support fronm the
# oar-metadata submodule).
#
set -e
prog=`basename $0`
execdir=`dirname $0`
[ "$execdir" = "" -o "$execdir" = "." ] && execdir=$PWD

base=`(cd $execdir/.. > /dev/null 2>&1; pwd)`
oarmd_pkg=$base/metadata

[ -d "$oarmd_pkg" -a -d "$oarmd_pkg/python/nistoar" ] || {
    echo "$prog: Missing metadata submodule"
    echo Clone the oar-metadata repo in this directory\; name it "'metadata'"
    exit 3
}

. $oarmd_pkg/scripts/_install-env.sh

#install the PDR python library
mkdir -p $PY_LIBDIR
echo Installing python libraries into $PY_LIBDIR...
(cd $PY_LIBDIR && PY_LIBDIR=$PWD)
(cd $SOURCE_DIR/python && python3 setup.py install --install-purelib=$PY_LIBDIR --install-scripts=$BINDIR)

#install the JAVA jars
# None at this time

$oarmd_pkg/scripts/install_extras.sh --install-dir=$INSTALL_DIR

mkdir -p $INSTALL_DIR/var/logs
echo cp -r $SOURCE_DIR/etc $INSTALL_DIR
cp -r $SOURCE_DIR/etc $INSTALL_DIR

mkdir -p $INSTALL_DIR/docs
cp $SOURCE_DIR/docs/*-openapi.yml $SOURCE_DIR/docs/*-elements.html $INSTALL_DIR/docs
