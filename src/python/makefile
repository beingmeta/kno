PYINC=/usr/include/python2.5/
PYLIB=/usr/lib/python2.5/

parzeltounge.so: parzeltounge.c
	gcc parzeltounge.c -g -fPIC -I$(PYINC) -L$(PYLIB) -shared \
	 -o parzeltounge.so \
		-lfdtype -lfddb -lfddbfile -lfdscheme -lfdschemeio \
		-lfdweb -ltexttools -lfdbserv -lfdschemeio \
		-lu8fns -lu8 -lu8io -lz \
		-Xlinker -export-dynamic -Wl,-O1 -Wl,-Bsymbolic-functions \
		-lpython2.5




