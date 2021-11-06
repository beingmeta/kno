#!/bin/sh

use_kno() {
    local arg = ${1:-${KNO_DEFAULT}}
    local knoroot=$(abspath $arg)
    if [ ${knoroot#/} == ${knoroot} ]; then
	knoroot="$(pwd)/${knoroot}";
    fi;
    export PATH=${knoroot}/bin:${PATH};
    export KNO_SYSROOT=${knoroot};
}
