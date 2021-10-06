# How to build Kno

The build process for Kno uses `autoconf` and `make` in a relatively
straightforward manner. A top level `build` script/command bundles
these together, so it may be enough to just go to the KNO root
directory and type `build` (with possible arguments) providing all the
neccessary dependencies are in place.

A slightly less automagic way to build Kno is to just configure and
make:
````shell
configure *flags*
````

`configure --help` shows you the options for the Kno configuration
script, but for convenience, we list some of them here.
````shelldoc
  --prefix=*dir*          where to install architecture-*independent* files
  --exec-prefix=*dir*     where to install architecture-*dependent* files
  --bindir=DIR            user executables [EPREFIX/bin]
  --sbindir=DIR           system admin executables [EPREFIX/sbin]
  --libdir=DIR            object code libraries [EPREFIX/lib]
  --includedir=DIR        C header files [PREFIX/include]
  --mandir=DIR            man documentation [DATAROOTDIR/man]
  --docdir=DIR            documentation root [DATAROOTDIR/doc/PACKAGE]

  --with-daemonid The identity (user) for Kno servers to run as
  --with-rundir The default location for server state files
  --with-logdir The default location for log files
  --with-bugjar The default location for bugjar reports

  --with-malloc=*module* Use custom malloc (if available).

  --with-statedir where to keep local state e.g. /var
  --with-execwrapper program to use when calling exec()
  --without-chowning
  --without-sudo use sudo with install
  --with-mkdir command
  --with-chmod command
  --with-chown command
  --with-chgrp command
  --with-deletefile command
  --with-copyfile command
  --with-touchfile command
  --with-apxs command
  --with/out-make-target default-target
  --with/out-suffix string to use as a suffix on executables and libraries
  --with-optinclude Use optional include directory
  --with-optlib Use optional library directory
  --with-sourcedir source directory
  --with-extra_sources Configure extra source directories for gdb/lldb
  --with-optdir Use optional header/lib
  --with-extlib[=DIR]       use additional lib dir
  --with-rpath dir
  --with-fileinfo record fileinfo
  --with/out-stackcheck Build with native stack checking
  --with/out-profiling compile to generate profile information
  --with/out-duma Build with Detect Unintended Memory Access


  --disable-htmldump disable code for dumping HTML backtraces
  --disable-prefetching don't have frame operations prefetch
  --disable-cffi disable the C FFI interface
  --disable-xprofiling disable provisions for extended rusage-based profiling
  --disable-shared Build without shared libraries
  --disable-async Disable async I/O features in server/servlet code
  --disable-dtblock DTBlock dtype for net I/O
  --enable-thread-debug Use libu8's thread tracing and debugging facilities
  --enable-ipeval Enable IPEVAL query optimization

  --en/disable-fileconfig enable file-based configuration

  --disable-leveldb Build and install LevelDB module
  --disable-rocksdb Build and install Rocksdb module
  --disable-odbc Build odbc modules

  --with/out-crypto Compile the crypto module
  --with/out-global-ipeval Use global lock and variables for ipeval
  --with/out-curl Use libcurl in webtools
  --with/out-dns Use dns in web
  --with/out-codename for distribution
  --with/out-admin-group Install files with admin group and group write permissions
  --with/out-webuser The user for the webserver
  --with/out-webgroup The group for the webserver
  --with/out-apacheinfo where Apache module info lives
  --with/out-apachelib where Apache extensions live
  --with/out-arch build with particular --arch flags
  --with/out-share-dir location
  --with/out-data-dir location
  --with/out-config-dir location
  --with/out-local-config location
  --with/out-shared-config location
  --with/out-unpackage-dir location
  --with/out-installed-module-dir location
  --with/out-local-modules location
  --with/out-stdlib-module-dir location
  --with/out-loadpath location
  --with/out-libscm location
  --with/out-module-path location
  --with/out-boot-config configstring
  --with/out-dload-path location
  --with/out-i18n built and install message catalogs

  --with/out-mmap build without using mmap
  --with/out-tls Force use of threadlocal storage
  --with-nptrlocks Sets the number of pointer hash locks to use
  --without-editline Use the editline with the kno console
  --with-libu8 Use libu8 includes

  --without-parseltongue Don't build parseltongue libraries for
    connecting Kno and Python

  --with/out-mysql Use alternate mysql lib
  --with/out-sundown Compile the sundown module
  --with/out-exif Use libexif in webtools
  --with/out-qrcode Create qrcode binary module
  --with/out-bootsystem Setup boot method
  --with/out-systemd Specify systemd install directory
````
