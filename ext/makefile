FDCONSOLE=$(which fdconsole)
EXEPREFIX=$(dirname ${FDCONSOLE})
PREFIX=$(dirname ${EXECPREFIX})

libpng:
	git clone git://git.code.sf.net/p/libpng/code libpng
libqrencode:
	git clone https://github.com/fukuchi/libqrencode.git libqrencode

update-libpng:
	cd libpng; git pull
	cd libpng; autoconf; configure --prefix=${PREFIX}; make


