%.image: Dockerfile.%
	docker build -t kno:$* -f $< . && \
	docker tag kno:$* bemeta/kno:$* && \
	touch $@

%.pushed: %.image
	docker push bemeta/kno:$* && \
	touch $@


default: full.image base.image

full.image: base.image
dev.image: full.image
brico.image: full.image

