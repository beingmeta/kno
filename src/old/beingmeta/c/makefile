prefix		=`fdconfig prefix`
exec_prefix	=`fdconfig exec_prefix`
BINDIR		=`fdconfig bin`
LIBDIR		=`fdconfig lib`
ETC		=`fdconfig etc`
CFLAGS		=`fdconfig cflags`
LDFLAGS		=`fdconfig ldflags`
LIBS		=`fdconfig libs`
LIBINSTALL	=`fdconfig cmodules`
INCLUDE		=`fdconfig include`
FDVERSION	=`fdconfig version`
FDMAJOR		=`fdconfig major`
LIBSUFFIX	=`fdconfig libsuffix`
MACLIBTOOL	= $(CC) -dynamiclib -single_module \
			-undefined dynamic_lookup \
			$(LDFLAGS) $(LIBS)
MSG		= echo
MKSO		= $(CC) -shared $(LDFLAGS) $(LIBS)
MKSTATIC	= ld -r
BMSTACK_OBJS=src/x2vec.o
TAGGER_OBJS=tagger/tagger.o tagger/ofsm.o tagger/tagxtract.o tagger/taglink.o
TAGGER_SOURCES=tagger/tagger.c tagger/tagger.h		\
	tagger/ofsm.c tagger/tagxtract.c tagger/taglink.c

src/%.o: src/%.c etc/fileinfo
	$(CC) $(CFLAGS) -D_FILEINFO="`etc/fileinfo $<`" -o $@ -c $<
	@$(MSG) CC $@ $<

%.a:
	$(MKSTATIC) -o $
%.so: src/%.o
	$(MKSO) -o $@ $<
	@$(MSG) MKSO $@ $<
%.dylib:
	@$(MACLIBTOOL) -install_name \
		@rpath/lib/`basename $(@F) .dylib`.$(FDMAJOR).dylib \
		$(DYLIB_FLAGS) -o $@ $< # -ltexttools
	@$(MSG) MACLIBTOOL $@

install: install-dev
install-libs: x2vec.${LIBSUFFIX} tagger.${LIBSUFFIX}
	install x2vec.so ${LIBINSTALL}/x2vec.${LIBSUFFIX}.${FDVERSION}
	ln -sf x2vec.so.${FDVERSION} ${LIBINSTALL}/x2vec.so.${FDMAJOR}
	ln -sf x2vec.so.${FDVERSION} ${LIBINSTALL}/x2vec.so.${FDMAJOR}.${FDMINOR}
	install tagger.so ${LIBINSTALL}/tagger.so.${FDVERSION}
	ln -sf tagger.so.${FDVERSION} ${LIBINSTALL}/tagger.so.${FDMAJOR}
	ln -sf tagger.so.${FDVERSION} ${LIBINSTALL}/tagger.so.${FDMAJOR}.${FDMINOR}
install-dev: install-libs
	ln -sf tagger.so.${FDVERSION} ${LIBINSTALL}/tagger.so
	ln -sf x2vec.so.${FDVERSION} ${LIBINSTALL}/x2vec.so

libtagger.a: $(TAGGER_OBJS)
	$(MKSTATIC) -o $@ $(TAGGER_OBJS)
	@$(MSG) MKSTATIC $@ $(TAGGER_OBJS)
libbmstack.a: $(BMSTACK_OBJS)
	$(MKSTATIC) -o $@ $<
	@$(MSG) MKSTATIC $@ $<

tagger.so: $(TAGGER_OBJS)
	@$(MKSO) -o $@ $(TAGGER_OBJS)
	@$(MSG) MKSO $@ $(TAGGER_OBJS)
tagger.dylib: $(TAGGER_OBJS)
	@$(MACLIBTOOL) -install_name \
		@rpath/lib/`basename $(@F) .dylib`.$(FDMAJOR).dylib \
		$(DYLIB_FLAGS) -o $@ $(TAGGER_OBJS) # -ltexttools
	@$(MSG) MACLIBTOOL $@

fileinfo: ../etc/fileinfo.c
	$(CC) -o fileinfo ../etc/fileinfo.c

clean:
	rm -f fileinfo src/*.o tagger/*.o *.so

x2vec.o: src/x2vec.h

