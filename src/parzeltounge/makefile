PYINC=/usr/include/python2.7/
PYLIB=/usr/lib/python2.7/

parzeltounge.so: parzeltounge.c
	gcc parzeltounge.c -g -fPIC -I$(PYINC) -I../../include -L$(PYLIB) -shared \
	 -o parzeltounge.so \
		-lfdcore -lfdstorage -lfddrivers -lfdscheme \
		-lfdweb -ltexttools -lfddbserv \
		-lu8fns -lu8 -lu8io -lz \
		-Xlinker -export-dynamic -Wl,-O1 -Wl,-Bsymbolic-functions \
		-lpython2.7




