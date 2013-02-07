source ../etc/gdbinit
directory ../src/dtype:../src/db:../src/scheme:../src/texttools:../src/fdweb:../include/fdb
set env DYLD_LIBRARY_PATH ../lib
set env LD_LIBRARY_PATH ../lib
set env FD_DLOADPATH ../lib/framerd/%.so:../lib/framerd/%.dylib
