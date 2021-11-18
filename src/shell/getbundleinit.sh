#!/bin/sh

if [ "${0#-}" != "$0" ]; then
    echo "Couldn't determine environment from command name $0";
    exit;
fi;
OUTPUT=${1:-/tmp/bundleinit.out}
cmd_root=$(dirname $0)
if [ "${cmd_root#/}" != "${cmd_root}" ]; then
    :;
elif [ "${cmd_root}" = "." ]; then
    cmd_root="$(pwd)/";
elif [ "${cmd_root}" = ".." ]; then
    cmd_root="$(dirname $(pwd))/";
elif [ "${cmd_root#./}" != "${cmd_root}" ]; then
    cmd_root="$(pwd)/${cmd_root#./}/";
elif [ "${cmd_root#../}" != "${cmd_root}" ]; then
    cmd_root="$(dirname $(pwd))/${cmd_root#../}/";
else
    echo "# Couldn't determine command root for KNO inits";
    exit;
fi;

cat > ${OUTPUT} <<EOF
if [ $(basename $0) = "usebundle.sh" ]; then
    echo "# Warning: You seem to be running usebundle.sh as a command, which is probably not what you want.";
    echo "#   You will want to load (source) this file instead of using it as a command.";
fi
EOF

echo "PATH=${cmd_root}/bin:\${PATH};" >> ${OUTPUT};
echo "SYSROOT=${cmd_root};" >> ${OUTPUT};
if [ "${OUTPUT}" = "/tmp/bundleinit.out" ]; then
    cat ${OUTPUT};
fi;
