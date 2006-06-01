PYINC=/usr/include/python2.3/
PYLIB=/usr/lib/python2.3/

framerd.so: framerd.c
	gcc framerd.c -g -fPIC -I$(PYINC) -L$(PYLIB) -shared -o framerd.so -lfddbfile -lfddb -lfdlisp -lfdbase0 -lu8fns -lu8 -lz


