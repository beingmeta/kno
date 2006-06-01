# -*- Mode: Makefile; -*-
# Copyright (C) 2004, 2005 beingmeta, inc.
# This file is a part of beingmeta's FramerD implementation, EFramerD.
MAKEFILE_VERSION="$Id: makefile.in,v 1.40 2006/01/07 04:23:29 haase Exp $

CC	= gcc
XCFLAGS=
CFLAGS=-fPIC -Wno-pointer-sign -g -O2  -D_REENTRANT -dynamic \
	-I./include -I../../include $(XCFLAGS)
LDFLAGS= $(EFENCE)  -L../../lib 
LIBS=-lcurl -lu8  -lgcc -lu8io -lu8data -lu8stdio -lu8 -lu8fns -lz -lm

DESTDIR		=
prefix		= /usr
exec_prefix	= ${prefix}
LIBINSTALLDIR	= $(DESTDIR)${exec_prefix}/lib
CLEAN		= @CLEAN@
INSTALL		= /usr/bin/install -c
RANLIB		= ranlib
VALGRIND	= valgrind --tool=memcheck --leak-check=yes --num-callers=9
TESTPROG	= 
RUNENV		= 
RUNTEST		= $(RUNENV)$(TESTPROG)
CHOICEFLAGS	= 

TESTBIN=makedtype printdtype copydtype testcapi \
        choicetest slotmaptest \
	hashtabletest \
	intersectiontest uniontest \
	differencetest \
        maketable resizetable \
	tablekeys readtable

all: $(TESTBIN)

memtests:
	make "TESTPROG=$(VALGRIND)" mosttests
alltests: dtypetests choicetests mergechoicetests \
		tabletests slotmaptests slotmaptests2 \
		smallbricotests mediumbricotests bigbricotests
mosttests: dtypetests choicetests tabletests slotmaptests slotmaptests2 \
		smallbricotests
dtypetests:
	$(RUNTEST) ./testcapi
	$(RUNTEST) ./makedtype fixnum.dtype 33
	$(RUNTEST) ./makedtype flonum.dtype 33.0
	$(RUNTEST) ./makedtype big.dtype 12345678987654321
	$(RUNTEST) ./makedtype googleplus.dtype 10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001
	$(RUNTEST) ./makedtype nil.dtype "()"
	$(RUNTEST) ./makedtype empty.dtype "{}"
	$(RUNTEST) ./makedtype bool.dtype \#f
	$(RUNTEST) ./makedtype oid.dtype @314/55
	$(RUNTEST) ./makedtype symbol.dtype foo
	$(RUNTEST) ./makedtype oddsymbol.dtype \|bar\|
	$(RUNTEST) ./printdtype fixnum.dtype
	$(RUNTEST) ./printdtype flonum.dtype
	$(RUNTEST) ./printdtype big.dtype
	$(RUNTEST) ./printdtype nil.dtype
	$(RUNTEST) ./printdtype empty.dtype
	$(RUNTEST) ./printdtype bool.dtype
	$(RUNTEST) ./printdtype oid.dtype
	$(RUNTEST) ./printdtype symbol.dtype
	$(RUNTEST) ./printdtype oddsymbol.dtype
	$(RUNTEST) ./printdtype ../data/testobj.dtype
	$(RUNTEST) ./copydtype ../data/testobj.dtype testobj.dtype
	$(RUNTEST) ./printdtype ../data/testobj.dtype
	$(RUNTEST) ./printdtype testobj.dtype
	$(RUNTEST) ./makedtype temp.dtype -f../data/testobj.text
	diff ../data/testobj.dtype testobj.dtype
	diff temp.dtype testobj.dtype

mergechoicetests:
	make "CHOICEFLAGS=MERGECHOICES=20" choicetests
choicetests: intchoicetests oidchoicetests objchoicetests compchoicetests
intchoicetests: ../data/intall.data
	rm -f intersection.data nintersection.data \
	      union.data union.ndata diff.data diff.ndata
	$(RUNTEST) ./intersectiontest $(CHOICEFLAGS) intintersection.data ../data/int?.data
	sort intintersection.data > intintersection.sorted
	sort ../data/intcommon.data | diff - intintersection.sorted
	$(RUNTEST) ./uniontest $(CHOICEFLAGS) intunion.data ../data/int?.data
	sort intunion.data > intunion.sorted
	sort ../data/intmerged.data | diff - intunion.sorted
	$(RUNTEST) ./differencetest $(CHOICEFLAGS) intdiff.data \
                    ../data/intall.data intunion.sorted
	sort intdiff.data > intdiff.sorted
	sort ../data/intmissing.data | diff - intdiff.sorted
oidchoicetests: ../data/oidall.data
	$(RUNTEST) ./intersectiontest $(CHOICEFLAGS) oidintersection.data ../data/oid?.data
	sort oidintersection.data > oidintersection.sorted
	sort ../data/oidcommon.data | diff - oidintersection.sorted
	$(RUNTEST) ./uniontest $(CHOICEFLAGS) oidunion.data ../data/oid?.data
	sort oidunion.data > oidunion.sorted
	sort ../data/oidmerged.data | diff - oidunion.sorted
	$(RUNTEST) ./differencetest $(CHOICEFLAGS) oiddiff.data \
                    ../data/oidall.data oidunion.sorted
	sort oiddiff.data > oiddiff.sorted
	sort ../data/oidmissing.data | diff - oiddiff.sorted
# Note that we don't do diffing here because some of the objects
# are slotmaps which aren't canonically ordered
objchoicetests: ../data/objall.data
	$(RUNTEST) ./intersectiontest $(CHOICEFLAGS) objintersection.data ../data/obj?.data
	$(RUNTEST) ./uniontest $(CHOICEFLAGS) objunion.data ../data/obj?.data
	$(RUNTEST) ./differencetest $(CHOICEFLAGS) objdiff.data \
                    ../data/objall.data objunion.data
compchoicetests: ../data/compall.data
	$(RUNTEST) ./intersectiontest $(CHOICEFLAGS) compintersection.data ../data/comp?.data
	$(RUNTEST) ./uniontest $(CHOICEFLAGS) compunion.data ../data/comp?.data
	$(RUNTEST) ./differencetest $(CHOICEFLAGS) compdiff.data \
                    ../data/compall.data compunion.data

../data/intall.data:
	cd ../data; make intall.data
../data/oidall.data:
	cd ../data; make oidall.data
../data/objall.data:
	cd ../data; make objall.data
../data/compall.data:
	cd ../data; make compall.data

tabletests:
	rm -f temp.table
	$(RUNTEST) ./hashtabletest temp.table foo 33
	$(RUNTEST) ./hashtabletest temp.table foo +444
	$(RUNTEST) ./hashtabletest temp.table foo +5555
	$(RUNTEST) ./hashtabletest temp.table foo
	$(RUNTEST) ./hashtabletest temp.table bar
	$(RUNTEST) ./hashtabletest temp.table bar 88
	$(RUNTEST) ./hashtabletest temp.table bar 99
	$(RUNTEST) ./hashtabletest temp.table foo -444
	$(RUNTEST) ./hashtabletest temp.table foo
	$(RUNTEST) ./hashtabletest temp.table bar +66
	$(RUNTEST) ./hashtabletest temp.table bar "+\"sixty-six\""
	$(RUNTEST) ./hashtabletest temp.table 33 foo
	$(RUNTEST) ./hashtabletest temp.table 444 foo
	$(RUNTEST) ./hashtabletest temp.table 5555 +foo
	$(RUNTEST) ./hashtabletest temp.table 5555
	$(RUNTEST) ./hashtabletest temp.table 444
	$(RUNTEST) ./hashtabletest temp.table 33
	$(RUNTEST) ./hashtabletest temp.table foo
	$(RUNTEST) ./hashtabletest temp.table bar
	$(RUNTEST) ./hashtabletest temp.table "(A . B)"
	$(RUNTEST) ./hashtabletest temp.table "(A . B)" cons
	$(RUNTEST) ./hashtabletest temp.table "(A . B)" pair
	$(RUNTEST) ./hashtabletest temp.table "(A . B)" +2
	$(RUNTEST) ./hashtabletest temp.table "(A . B)" +99
	$(RUNTEST) ./hashtabletest temp.table "(A . B)" "+\"string\""
	$(RUNTEST) ./hashtabletest temp.table "(A . B)"

slotmaptests:
	rm -f temp.smap
	$(RUNTEST) ./slotmaptest temp.smap foo 33
	$(RUNTEST) ./slotmaptest temp.smap foo +444
	$(RUNTEST) ./slotmaptest temp.smap foo +5555
	$(RUNTEST) ./slotmaptest temp.smap foo
	$(RUNTEST) ./slotmaptest temp.smap bar
	$(RUNTEST) ./slotmaptest temp.smap bar 88
	$(RUNTEST) ./slotmaptest temp.smap bar 99
	$(RUNTEST) ./slotmaptest temp.smap foo -444
	$(RUNTEST) ./slotmaptest temp.smap foo
	$(RUNTEST) ./slotmaptest temp.smap bar +66
	$(RUNTEST) ./slotmaptest temp.smap bar "+\"sixty-six\""
	$(RUNTEST) ./slotmaptest temp.smap 33 foo
	$(RUNTEST) ./slotmaptest temp.smap 444 foo
	$(RUNTEST) ./slotmaptest temp.smap 5555 +foo
	$(RUNTEST) ./slotmaptest temp.smap 5555
	$(RUNTEST) ./slotmaptest temp.smap 444
	$(RUNTEST) ./slotmaptest temp.smap 33
	$(RUNTEST) ./slotmaptest temp.smap foo
	$(RUNTEST) ./slotmaptest temp.smap bar

slotmaptests2:
	rm -f temp.smap2
	$(RUNTEST) ./slotmaptest temp.smap2 foo 33
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)"
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)" cons
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)" pair
	$(RUNTEST) ./slotmaptest temp.smap2 foo +444
	$(RUNTEST) ./slotmaptest temp.smap2 foo +5555
	$(RUNTEST) ./slotmaptest temp.smap2 foo
	$(RUNTEST) ./slotmaptest temp.smap2 bar
	$(RUNTEST) ./slotmaptest temp.smap2 bar 88
	$(RUNTEST) ./slotmaptest temp.smap2 bar 99
	$(RUNTEST) ./slotmaptest temp.smap2 foo -444
	$(RUNTEST) ./slotmaptest temp.smap2 foo
	$(RUNTEST) ./slotmaptest temp.smap2 bar +66
	$(RUNTEST) ./slotmaptest temp.smap2 bar "+\"sixty-six\""
	$(RUNTEST) ./slotmaptest temp.smap2 33 foo
	$(RUNTEST) ./slotmaptest temp.smap2 444 foo
	$(RUNTEST) ./slotmaptest temp.smap2 5555 +foo
	$(RUNTEST) ./slotmaptest temp.smap2 5555
	$(RUNTEST) ./slotmaptest temp.smap2 444
	$(RUNTEST) ./slotmaptest temp.smap2 33
	$(RUNTEST) ./slotmaptest temp.smap2 foo
	$(RUNTEST) ./slotmaptest temp.smap2 bar
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)" +2
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)" +99
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)" "+\"string\""
	$(RUNTEST) ./slotmaptest temp.smap2 "(A . B)"

smallbricotests: ../data/bricosmall.dump
	$(RUNTEST) ./maketable ../data/bricosmall.dump bricosmall.table
	$(RUNTEST) ./readtable bricosmall.table
mediumbricotests: ../data/bricosample.dump
	$(RUNTEST) ./maketable ../data/bricosample.dump bricosample.table
	$(RUNTEST) ./readtable bricosample.table
bigbricotests: ../data/brico.dump
	$(RUNTEST) ./maketable ../data/brico.dump brico.table
	$(RUNTEST) ./readtable brico.table

../data/bricosmall.dump:
	cd ../data; make BRICO=/data/brico/brico bricosmall.dump
../data/bricosample.dump:
	cd ../data; make BRICO=/data/brico/brico bricosample.dump
../data/brico.dump:
	cd ../data; make BRICO=/data/brico/brico brico.dump

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

$(TESTBIN): ../../src/db/dtypestream.o ../../lib/libfdtype.a
%: %.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

# This is for making shared test programs, when there are associated problems
%.shared: %.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ \
		../../src/db/dtypestream.o $(LIBS) -lfdtype 

../../lib/libfdtype.a:
	cd ../..; make lib/libfdtype.a
../../src/db/dtypestream.o:
	cd ../..; make lib/libfddb.a

clean:
	rm -f u8makedtype $(TESTBIN)
	rm -f *.data *.dump *.table *.sorted
