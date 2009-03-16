PYINC=/usr/include/python2.5/
PYLIB=/usr/lib/python2.5/

framerd.so: framerd.c
	gcc framerd.c -g -fPIC -I$(PYINC) -L$(PYLIB) -shared -o framerd.so \
		-lfdtype -lfddb -lfddbfile -lfdscheme -lfdschemeio \
		-lfdweb -ltexttools -lfdbserv -lfdschemeio \
		-lu8fns -lu8 -lu8io -lz



