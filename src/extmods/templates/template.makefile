#!/usr/bin/env make
KNOCONFIG         = knoconfig
KNOBUILD          = knobuild
MONGOCINSTALL     = mongoc-install

prefix		::= $(shell ${KNOCONFIG} prefix)
libsuffix	::= $(shell ${KNOCONFIG} libsuffix)
KNO_CFLAGS	::= -I. -fPIC $(shell ${KNOCONFIG} cflags)
KNO_LDFLAGS	::= -fPIC $(shell ${KNOCONFIG} ldflags)
KNO_LIBS	::= $(shell ${KNOCONFIG} libs)
CMODULES	::= $(DESTDIR)$(shell ${KNOCONFIG} cmodules)
INSTALLMODS	::= $(DESTDIR)$(shell ${KNOCONFIG} installmods)
LIBS		::= $(shell ${KNOCONFIG} libs)
LIB		::= $(shell ${KNOCONFIG} lib)
INCLUDE		::= $(shell ${KNOCONFIG} include)
KNO_VERSION	::= $(shell ${KNOCONFIG} version)
KNO_MAJOR	::= $(shell ${KNOCONFIG} major)
KNO_MINOR	::= $(shell ${KNOCONFIG} minor)
PKG_VERSION     ::= $(shell cat ./version)
PKG_MAJOR       ::= $(shell cat ./version | cut -d. -f1)
FULL_VERSION    ::= ${KNO_MAJOR}.${KNO_MINOR}.${PKG_VERSION}
PATCHLEVEL      ::= $(shell u8_gitpatchcount ./version)
PATCH_VERSION   ::= ${FULL_VERSION}-${PATCHLEVEL}

PKG_NAME	::= mongodb
DPKG_NAME	::= ${PKG_NAME}_${PATCH_VERSION}

SUDO            ::= $(shell which sudo)
INIT_CFLAGS     ::= ${CFLAGS}
INIT_LDFAGS     ::= ${LDFLAGS}
BSON_CFLAGS     ::= $(shell INSTALLROOT=${MONGOCINSTALL} etc/pkc --static --cflags libbson-static-1.0)
BSON_LDFLAGS    ::= $(shell INSTALLROOT=${MONGOCINSTALL} etc/pkc --static --libs libbson-static-1.0)
MONGODB_CFLAGS  ::= $(shell INSTALLROOT=${MONGOCINSTALL} etc/pkc --static --cflags libmongoc-static-1.0)
MONGODB_LDFLAGS ::= $(shell INSTALLROOT=${MONGOCINSTALL} etc/pkc --static --libs libmongoc-static-1.0)
CFLAGS		  = ${INIT_CFLAGS} ${KNO_CFLAGS} ${BSON_CFLAGS} ${MONGODB_CFLAGS}
LDFLAGS		  = ${INIT_LDFLAGS} ${KNO_LDFLAGS} ${BSON_LDFLAGS} ${MONGODB_LDFLAGS}

MKSO		  = $(CC) -shared $(LDFLAGS) $(LIBS)
SYSINSTALL        = /usr/bin/install -c
DIRINSTALL        = /usr/bin/install -d
MODINSTALL        = /usr/bin/install -m 0664
USEDIR        	  = /usr/bin/install -d
MSG		  = echo
MACLIBTOOL	  = $(CC) -dynamiclib -single_module -undefined dynamic_lookup \
			$(LDFLAGS)

GPGID             = FE1BC737F9F323D732AA26330620266BE5AFF294
CODENAME	::= $(shell ${KNOCONFIG} codename)
REL_BRANCH	::= $(shell ${KNOBUILD} getbuildopt REL_BRANCH current)
REL_STATUS	::= $(shell ${KNOBUILD} getbuildopt REL_STATUS stable)
REL_PRIORITY	::= $(shell ${KNOBUILD} getbuildopt REL_PRIORITY medium)
ARCH            ::= $(shell ${KNOBUILD} getbuildopt BUILD_ARCH || uname -m)
APKREPO         ::= $(shell ${KNOBUILD} getbuildopt APKREPO /srv/repo/kno/apk)
APK_ARCH_DIR      = ${APKREPO}/staging/${ARCH}

STATICLIBS=${MONGOCINSTALL}/lib/libbson-static-1.0.a \
	${MONGOCINSTALL}/lib/libmongoc-static-1.0.a

default:
	@make ${STATICLIBS}
	@make mongodb.${libsuffix}

mongo-c-driver/.git:
	@git submodule init
	@git submodule update

mongoc-build/Makefile: mongo-c-driver/.git
	${USEDIR} mongoc-build
	cd mongoc-build; cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
	      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
	      -DCMAKE_INSTALL_PREFIX=../mongoc-install \
	      ${CMAKE_FLAGS} \
	      ../mongo-c-driver

mongodb.o: mongodb.c mongodb.h makefile ${STATICLIBS}
	@$(CC) $(CFLAGS) -D_FILEINFO="\"$(shell u8_fileinfo ./$< $(dirname $(pwd))/)\"" -o $@ -c $<
	@$(MSG) CC "(MONGODB)" $@
mongodb.so: mongodb.o mongodb.h makefile
	@$(MKSO) -o $@ mongodb.o -Wl,-soname=$(@F).${FULL_VERSION} \
	          -Wl,--allow-multiple-definition \
	          -Wl,--whole-archive ${STATICLIBS} -Wl,--no-whole-archive \
		 $(LDFLAGS)
	@$(MSG) MKSO "(MONGODB)" $@

mongodb.dylib: mongodb.o mongodb.h
	@$(MACLIBTOOL) -install_name \
		`basename $(@F) .dylib`.${KNO_MAJOR}.dylib \
		$(DYLIB_FLAGS) $(BSON_LDFLAGS) $(MONGODB_LDFLAGS) \
		-o $@ mongodb.o 
	@$(MSG) MACLIBTOOL "(MONGODB)" $@

mongoc-install/lib/libbson-static-1.0.a mongoc-install/lib/libmongoc-static-1.0.a: mongoc-build/Makefile
	${USEDIR} mongoc-install
	make -C mongoc-build install
	if test -d mongoc-install/lib; then \
	  echo > /dev/null; \
	elif test -d mongoc-install/lib64; then \
	  ln -sf lib64 mongoc-install/lib; \
	else echo "No install libdir"; \
	fi
	touch ${STATICLIBS}
staticlibs: ${STATICLIBS}
mongodb.dylib mongodb.so: staticlibs

install: install-cmodule install-scheme
suinstall doinstall:
	sudo make install

${CMODULES}:
	@${DIRINSTALL} ${CMODULES}

install-cmodule: ${CMODULES}
	${SUDO} u8_install_shared ${PKG_NAME}.${libsuffix} ${CMODULES} ${FULL_VERSION} "${SYSINSTALL}"

${INSTALLMODS}/mongodb:
	${SUDO} ${DIRINSTALL} $@

install-scheme: ${INSTALLMODS}/mongodb
	${SUDO} ${MODINSTALL} scheme/mongodb/*.scm ${INSTALLMODS}/mongodb

clean:
	rm -f *.o *.${libsuffix} *.${libsuffix}*
deep-clean: clean
	if test -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;
	rm -rf mongoc-build install
fresh: clean
	make
deep-fresh: deep-clean
	make

gitup gitup-trunk:
	git checkout trunk && git pull

# Debian packaging

DEBFILES=changelog.base control.base compat copyright dirs docs install

debian: dist/debian/compat dist/debian/control.base dist/debian/changelog.base
	rm -rf debian
	cp -r dist/debian debian
	cd debian; chmod a-x ${DEBFILES}

debian/compat: dist/debian/compat
	rm -rf debian
	cp -r dist/debian debian

debian/changelog: debian/compat dist/debian/changelog.base
	cat dist/debian/changelog.base | \
		u8_debchangelog kno-${PKG_NAME} ${CODENAME} ${PATCH_VERSION} \
			${REL_BRANCH} ${REL_STATUS} ${REL_PRIORITY} \
	    > $@.tmp
	if test ! -f debian/changelog; then \
	  mv debian/changelog.tmp debian/changelog; \
	elif diff debian/changelog debian/changelog.tmp 2>&1 > /dev/null; then \
	  mv debian/changelog.tmp debian/changelog; \
	else rm debian/changelog.tmp; fi
debian/control: debian/compat dist/debian/control.base
	u8_xsubst debian/control dist/debian/control.base "KNO_MAJOR" "${KNO_MAJOR}"

dist/debian.built: makefile debian/changelog debian/control
	dpkg-buildpackage -sa -us -uc -b -rfakeroot && \
	touch $@

dist/debian.signed: dist/debian.built
	@if test "${GPGID}" = "none" || test -z "${GPGID}"; then  	\
	  echo "Skipping debian signing";				\
	  touch $@;							\
	else 								\
	  echo debsign --re-sign -k${GPGID} ../kno-${PKG_NAME}_*.changes;	\
	  debsign --re-sign -k${GPGID} ../kno-${PKG_NAME}_*.changes && 	\
	  touch $@;							\
	fi;

deb debs dpkg dpkgs: dist/debian.signed

debfresh: clean debclean
	rm -rf debian
	make dist/debian.signed

debinstall: dist/debian.signed
	${SUDO} dpkg -i ../kno-${PKG_NAME}_*.deb

debclean: clean
	rm -rf ../kno-${PKG_NAME}-* debian dist/debian.*

# Alpine packaging

staging/alpine:
	@install -d $@

staging/alpine/APKBUILD: dist/alpine/APKBUILD staging/alpine
	cp dist/alpine/APKBUILD staging/alpine

staging/alpine/kno-${PKG_NAME}.tar: staging/alpine
	git archive --prefix=kno-${PKG_NAME}/ -o staging/alpine/kno-${PKG_NAME}.tar HEAD

dist/alpine.setup: staging/alpine/APKBUILD makefile ${STATICLIBS} \
	staging/alpine/kno-${PKG_NAME}.tar
	if [ ! -d ${APK_ARCH_DIR} ]; then mkdir -p ${APK_ARCH_DIR}; fi && \
	( cd staging/alpine; \
		abuild -P ${APKREPO} clean cleancache cleanpkg && \
		abuild checksum ) && \
	touch $@

dist/alpine.done: dist/alpine.setup
	( cd staging/alpine; abuild -P ${APKREPO} ) && touch $@
dist/alpine.installed: dist/alpine.setup
	( cd staging/alpine; abuild -i -P ${APKREPO} ) && touch dist/alpine.done && touch $@


alpine: dist/alpine.done
install-alpine: dist/alpine.done

.PHONY: alpine

