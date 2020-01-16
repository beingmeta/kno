%.image: Dockerfile.%
	docker build -t kno:$* -f $< . && \
	docker tag kno:$* bemeta/kno:$* && \
	touch $@

%.pushed: %.image
	docker push bemeta/kno:$* && \
	touch $@

configs/brico/%: ../../src/stdlib/brico/server/%
	cp $< $@

default: base.image full.image
pushed: full.pushed base.pushed
clean:
	rm -f *.image *.pushed
fresh:
	make clean
	make default

full.image: base.image
dev.image: full.image
brico.image: full.image configs/brico/brico.fdz configs/brico/brico.service configs/brico/brico.cfg

data/brico.tar.bz2 data/wikid.tar.bz2:
	cd data; wget https://kno.s3.amazonaws.com/brico/latest/$(@F) $@
