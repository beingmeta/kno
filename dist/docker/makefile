%.image: Dockerfile.%
	docker build -t kno:$* -f $< . && \
	docker tag kno:$* bemeta/kno:$* && \
	touch $@

%.pushed: %.image
	docker push bemeta/kno:$* && \
	touch $@


default: base.image full.image
pushed: full.pushed base.pushed
clean:
	rm -f *.image *.pushed
fresh:
	make clean
	make default

full.image: base.image
dev.image: full.image
brico.image: full.image

