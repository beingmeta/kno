%.image: Dockerfile.%
	docker build -t kno:$* -f $< . && touch $@

full.image: base.image
dev.image: full.image
brico.image: full.image
