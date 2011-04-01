CFLAGS = -fshort-enums -O3 -I/usr/local/include
all:  mumorph.so

tidy:
	rm -f mumorph.o *~
clean: tidy
	rm -f mumorph.so

mumorph.so: mumorph.o
	ld -shared -o $@ mumorph.o -L/usr/lib

#nlscript: fdscript.c muparse.o
#	gcc -o nlscript fdscript.c muparse.o \
#            -lfdtext -lfdscript -lframerd -ldtypes -lpthread -ldl -lefence

