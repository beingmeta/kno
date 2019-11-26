%.image: Dockerfile.%
	docker build -t kno:$* -f $< . && touch $@
