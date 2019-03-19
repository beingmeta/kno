#!/bin/sh
INPUTS=${1:-./brico/}
OUTPUTS=${1:-./fresh/}
index/core.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} BRICOSOURCE=${INPUTS} ${INPUTS}/brico.pool
index/lattices.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} BRICOSOURCE=${INPUTS} ${INPUTS}/brico.pool
index/relations.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} BRICOSOURCE=${INPUTS} ${INPUTS}/brico.pool
index/english.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} BRICOSOURCE=${INPUTS} ${INPUTS}/brico.pool
index/words.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} BRICOSOURCE=${INPUTS} ${INPUTS}/brico.pool
index/termlogic.scm INDIR=${INPUTS} OUTDIR=${OUTPUTS} BRICOSOURCE=${INPUTS} ${INPUTS}/brico.pool
