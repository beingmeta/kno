FDSCRIPT=/usr/bin/fdscript
BRICO=/data/brico/brico

all: intall.data oidall.data objall.data compall.data brico
brico: bricotiny.dump bricosmall.dump bricosample.dump brico.dump

intall.data:
	rm -f int?.data
	if test -x $(FDSCRIPT); \
           then $(FDSCRIPT) gendata.fdx intall.data; \
           else ln -s premade/int*.data .; \
        fi
oidall.data:
	rm -f oid?.data
	if test -x $(FDSCRIPT); \
           then $(FDSCRIPT) gendata.fdx oidall.data; \
           else ln -s premade/oid*.data .; \
        fi
objall.data:
	rm -f obj?.data
	if test -x $(FDSCRIPT); \
           then $(FDSCRIPT) gendata.fdx objall.data; \
           else ln -s premade/obj*.data .; \
        fi
compall.data:
	rm -f comp?.data
	if test -x $(FDSCRIPT); \
           then $(FDSCRIPT) gendata.fdx compall.data; \
           else ln -s premade/comp*.data .; \
        fi

bricotiny.dump:
	rm -f bricotiny.dump
	if test -x $(FDSCRIPT); \
           then BRICO=$(BRICO) $(FDSCRIPT) gendata.fdx bricotiny.dump; \
           else ln -s premade/bricotiny.dump .; \
        fi
bricosmall.dump:
	rm -f bricosmall.dump
	if test -x $(FDSCRIPT); \
           then BRICO=$(BRICO) $(FDSCRIPT) gendata.fdx bricosmall.dump; \
           else ln -s premade/bricotiny.dump bricosmall.dump; \
        fi
bricosample.dump:
	rm -f bricosample.dump
	if test -x $(FDSCRIPT); \
           then BRICO=$(BRICO) $(FDSCRIPT) gendata.fdx bricosample.dump; \
           else ln -s premade/bricotiny.dump bricosample.dump; \
        fi
brico.dump:
	rm -f brico.dump
	if test -x $(FDSCRIPT); \
           then BRICO=$(BRICO) $(FDSCRIPT) gendata.fdx brico.dump; \
           else ln -s premade/bricotiny.dump brico.dump; \
        fi
clean:
	rm -f *.data *.dump
