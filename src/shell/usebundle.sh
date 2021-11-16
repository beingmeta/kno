export PATH=/opt/kno-2110.1.4//bin:${PATH}
export SYSROOT=/opt/kno-2110.1.4/
if [ $(basename $0) = "usebundle.sh" ]; then
    echo "# Warning: You seem to be running usebundle.sh as a command, which is probably not what you want.";
    echo "#   You will want to load (source) this file instead of using it as a command.";
fi;
