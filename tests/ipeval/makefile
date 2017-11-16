OUTDIR=./output
PARAMS=

#export PARAMS="SERVER=beingmeta.com TRIALCOUNT=10"
#export PARAMS="SERVER=gemini.dot.beingmeta.com TRIALCOUNT=10"
hyponyms: ${OUTDIR}/hyponyms/raw
${OUTDIR}/hyponyms/raw:
	rm -f ./logs/hyponyms.log
	dobench ${PARAMS} :hyponyms 1000 inputs/a/descent.samples $@ | tee -a ./logs/hyponyms.log

specls: ${OUTDIR}/specls/raw
${OUTDIR}/specls/raw:
	rm -f ./logs/specls.log
	dobench ${PARAMS} :getspecls 1000 inputs/a/descent.samples $@ | tee -a ./logs/specls.log

related: ${OUTDIR}/related/raw
${OUTDIR}/related/raw:
	rm -f ./logs/related.log
	dobench ${PARAMS} :getrelated 1000 inputs/a/bricodb.samples $@ | tee -a ./logs/related.log

disambig: ${OUTDIR}/disambig/raw
${OUTDIR}/disambig/raw:
	rm -f ./logs/disambig.log
	dobench ${PARAMS} :disambig 100 inputs/a/keylists.dtype $@ | tee -a ./logs/disambig.log

#dobench ${PARAMS} :getrefs 250 inputs/a/passages.dtype $1/getrefs/raw

