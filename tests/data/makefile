BRICO=/data/brico/brico
intall.data:
	rm -f int?.data
	fdscript gendata.fdx intall.data
oidall.data:
	rm -f oid?.data
	fdscript gendata.fdx oidall.data
objall.data:
	rm -f obj?.data
	fdscript gendata.fdx objall.data
compall.data:
	rm -f comp?.data
	fdscript gendata.fdx compall.data
bricosmall.dump:
	BRICO=$(BRICO) fdscript gendata.fdx bricosmall.dump
bricosample.dump:
	BRICO=$(BRICO) fdscript gendata.fdx bricosample.dump
brico.dump:
	BRICO=$(BRICO) fdscript gendata.fdx brico.dump
clean:
	rm -f *.data *.dump
