dist/metadebs.done: dist/metadebs.built
	touch dist/metadebs.done
dist/metadebs.uploaded: dist/metadebs.built
	cd dist/metadebs; for changes in *.changes; do \
	  dupload -c --nomail --to beingmeta $${change} && \
	  rm -f $${change}; \
	done && \
	rm -f *.deb && \
	touch ../metadebs.uploaded;
