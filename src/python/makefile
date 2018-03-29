PYINC=/usr/include/python2.7/
PYLIB=/usr/lib/python2.7/

parseltongue.so: parseltongue.c
	gcc parseltongue.c -g -fPIC -I$(PYINC) -I../../include	\
	    -L$(PYLIB) -shared 					\
	    -o parseltongue.so 					\
		-lfdcore -lfdstorage -lfddrivers -lfdscheme 	\
		-lfdweb -ltexttools -lfddbserv			\
		-lu8fns -lu8 -lu8io -lz -lzstd -lm		\
		-Xlinker -export-dynamic 			\
		-Wl,-O1 -Wl,-Bsymbolic-functions 		\
		-lpython2.7

clean:
	rm -f *.o *.so
