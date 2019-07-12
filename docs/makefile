RONN_MANUAL       = Kno
RONN_ORGANIZATION = beingmeta.com

PAGES=knoc.1 knox.1 kno_config.8

man/%: ronn/%.ronn
	ronn --roff < $< > $@
man.html/%.html: ronn/%.ronn
	ronn --html < $< > $@
man.html.include/%.html: ronn/%.ronn
	ronn --fragment < $< > $@

default: manpages htmlpages htmlinclude

man manpages:
	@for page in ${PAGES}; do make man/$${page}; done

html htmlpages man.html:
	@for page in ${PAGES}; do make man.html/$${page}.html; done

htmlinclude man.html.include:
	@for page in ${PAGES}; do make man.html.include/$${page}.html; done

.PHONY: man manpages man.html man.html.include

clean:
	rm man/*.*
	rm man.html/*.html
	rm man.html.include/*.html
