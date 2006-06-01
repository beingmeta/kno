# -*- Mode: Makefile; -*-
# Copyright (C) 2004, 2005 beingmeta, inc.
# This file is a part of beingmeta's FramerD implementation, EFramerD.
MAKEFILE_VERSION="$Id: makefile.in,v 1.29 2005/05/02 05:25:16 haase Exp $

CC	= gcc
XCFLAGS=
CFLAGS=-fPIC -Wno-pointer-sign -g -O2  -D_REENTRANT -dynamic \
	-I./include -I../../include $(XCFLAGS)
LDFLAGS = $(EFENCE)  -L../../lib
LIBS=-lcurl -lu8  -lgcc -lu8io -lu8data -lu8stdio -lu8fns -lu8 -lu8io -lz -lm

DESTDIR		=
prefix		= /usr
exec_prefix	= ${prefix}
LIBINSTALLDIR	= $(DESTDIR)${exec_prefix}/lib
CLEAN		= @CLEAN@
INSTALL		= /usr/bin/install -c
RANLIB		= ranlib
VALGRIND	= valgrind --tool=memcheck --leak-check=yes --num-callers=9
TESTPROG	= 
#RUNENV		= LD_LIBRARY_PATH=../../lib DYLD_LIBRARY_PATH=../../lib 
RUNENV		= 
RUNTEST		= $(RUNENV)$(TESTPROG)

NETBRICOPOOL	= bground@beingmeta.com
FILEBRICOPOOL	= /data/brico/brico.pool
ZFILEBRICOPOOL	= /data/bg/brico.pool
NETBRICOINDEX	= bground@beingmeta.com
FILEBRICOINDEX	= /data/brico/brico.index
ZFILEBRICOINDEX	= /data/bg/brico.index
BRICOPOOL	= $(FILEBRICOPOOL)
BRICOINDEX	= $(FILEBRICOINDEX)

TESTBIN=pool-get dtcall indexop index-get dbtool

all: $(TESTBIN) 

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

$(TESTBIN): ../../lib/libfddbfile.a ../../lib/libfddb.a ../../lib/libfdtype.a

%: %.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# This is for making shared test programs, when there are associated problems
%.shared: %.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS) \
		-lfdtype -lfddb -lfddbfile

../../lib/libfdtype.a:
	cd ../..; make lib/libfdtype.a
../../lib/libfddb.a:
	cd ../..; make lib/libfddb.a
../../lib/libfddbfile.a:
	cd ../..; make lib/libfddbfile.a

clean:
	rm -f $(TESTBIN)

pooltests:
	$(RUNTEST) ./pool-get $(BRICOPOOL) @1/13800
	$(RUNTEST) ./pool-get $(BRICOPOOL) @1/13800 @1/13ad9 \
			@1/1525d @1/1bbe8 @1/1bdff @1/1cb47
	$(RUNTEST) ./pool-get $(BRICOPOOL) @/brico/13800 @/brico/13ad9 \
			@/brico/1525d @/brico/1bbe8 @/brico/1bdff @/brico/1cb47

filepooltests:
	make BRICOPOOL=$(FILEBRICOPOOL) pooltests
zfilepooltests:
	make BRICOPOOL=$(ZFILEBRICOPOOL) pooltests
netpooltests:
	make BRICOPOOL=$(NETBRICOPOOL) pooltests

allpooltests: filepooltests zfilepooltests netpooltests

indextests:
	$(RUNTEST) ./index-get $(BRICOINDEX) "(@1/2c1c7 . \"example\")"
	$(RUNTEST) ./index-get $(BRICOINDEX) "(@1/2c1c7 . \"example\")" \
		"(@1/2c1c7 . \"fish\")" "(@1/2c1c7 . \"dog\")"

fileindextests:
	make BRICOINDEX=$(FILEBRICOINDEX) indextests
zfileindextests:
	make BRICOINDEX=$(ZFILEBRICOINDEX) indextests
netindextests:
	make BRICOINDEX=$(NETBRICOINDEX) indextests

allindextests: fileindextests zfileindextests netindextests
