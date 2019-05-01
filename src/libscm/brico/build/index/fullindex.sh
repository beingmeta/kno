#!/bin/sh
POOL=${1:-./brico/brico.pool}
INPUTS=$(dirname ${POOL})
OUTPUTS=${2:-./fresh/}
if [ ! -z "${BRICOSOURCE}" ]; then
    export FD_BRICOSOURCE=${BRICOSOURCE};
elif test -f ./brico/brico.pool; then
    export FD_BRICOSOURCE=./brico/;
fi
index/core.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} ${POOL}
index/lattices.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} ${POOL}
index/relations.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} ${POOL}
index/words.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} ${POOL}
index/termlogic.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} ${POOL}
