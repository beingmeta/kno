AC_PREREQ(2.50)
AC_INIT(include/kno/lisp.h)
AC_PREFIX_PROGRAM(autoconf)

AC_DEFUN([AC_CHECK_USER],[
AC_MSG_CHECKING([for user $1])
  found_user="none";
  for user in $2;		    
  do
  if grep ^$user: /etc/passwd > /dev/null; 
  then
	found_user=$user;
	break;
  fi;
  done;
  $1=$found_user;
  AC_MSG_RESULT($found_user)])

AC_DEFUN([AC_CHECK_GROUP],[
AC_MSG_CHECKING([for group $1])
  found_group="none";
  for group in $2;
  do if grep ^$group: /etc/group > /dev/null; 
  then
	found_group=$group;
	break;
  fi; done;
  $1=$found_group;
  AC_MSG_RESULT($found_group)])

# Override the default value for localstatedir
AC_ARG_WITH(statedir,--with-statedir where to keep local state e.g. /var,localstatedir=$withval,localstatedir="/var")

CC=${CC}
DEFAULTMAKETARGET=update
threads_default="yes"
shared_default="yes"
shared_suffix="so"
rpath_default=$exec_prefix
tls_default="no"
global_ipeval_default="no"
enable_ipeval_default="no"
install_dir_opts="-d -m 0775"
install_file_opts="-m 0664"
install_exe_opts="-m 0775"
webuser=""
webgroup=""
wwwinstall=""
admin_group=""
admininstall=""
knouser=""
knouserinstall=""
libc_pthreads="no"
libc_dlopen="no"
calltracko=""
curlo=""
ldnso=""
ipevalo=""
ipevalopso=""
curlconfig=""
curl_default="no"
dns_default="no"
exifo=""
doi18n="no"
rpmflags=""
rmpdir="dist"
exif_default="no"
dynamic_cflag=""
xstatic_exe_libs=""
xstatic_scheme_libs=""
xdynamic_scheme_libs=""
scheme_builtins_init="kno_init_scheme()"
definefileinfo=""
versionsh=""
suffix=""
rpath=""
daemon_identity_default="daemon"
setup_target="setup-unix"
extra_sources=
DREENTRANT="-D_REENTRANT"
MKSTATIC="ld -r "
TESTLIBS=
TESTLDFLAGS=
STATICLDFLAGS=
EXEFLAGS=
EXELIBS=
BINPATH=${PATH}
RPATHFLAGS="-Wl,'-rpath \$(RPATH)'"
SHARED_LIB_TARGET=shared-libs
KNO_PATCHVERSION=$(if test -f REVISION; then cat REVISION; elif test -d .git; then u8_gitversion etc/base_version; fi)
KNO_VERSION=$(echo ${KNO_PATCHVERSION} | cut -d'-' -f1)
KNO_MAJOR=$(echo ${KNO_VERSION} | cut -d'.' -f1)
KNO_MAJOR=$(echo ${KNO_VERSION} | cut -d'.' -f1)
KNO_MINOR=$(echo ${KNO_VERSION} | cut -d'.' -f2)
KNO_RELEASE=$(echo ${KNO_VERSION} | cut -d'.' -f3)
KNO_PATCHLEVEL=$(echo ${KNO_PATCHVERSION} | cut -d'-' -f2)
VERSION=$KNO_VERSION
RPMVERSION=${KNO_RELEASE}
DEBVERSION=${KNO_RELEASE}
DEB_BUILD_TARGET=dist/debs.signed
LIBSOURCE=lib
EXESOURCE=exe
STATICEXE=static
mshared_suffix=so.${KNO_MAJOR}
mmshared_suffix=so.${KNO_MAJOR}.${KNO_MINOR}
vshared_suffix=so.${KNO_VERSION}
CLEAN="rm -f "
I18N=""
INSTALLI18N=""
WWWUSER="apache"
apache_modules_dir="/usr/lib/apache2/modules"
apache_conf_dir="/etc/apache2"
apache_log_dir=""
apache_modinfo="/etc/apache2"
APXSFLAGS=
A2EN="none"
stdlib_module_dir=""
installed_module_dir=""
local_module_dir=""
default_module_path=""
module_path=""
source_dir=""
EXTMODS="hyphenate imagetools leveldb mod_knocgi mongodb mysql nng odbc rocksdb sundown tidy zeromq ziptools"
CMODULES=""
INSTALL_CMODULES=""
TEST_CMODULES=""
DATAFILES="etc/base_version"
SUDO=""
DOINSTALL=
RPMDIR=${RPMDIR:-dist}
RPMFLAGS=${RPMFLAGS:-}
APTLY=
APTLY_FLAGS=
PKGCONFIG_DIR=
GPG=`which gpg2 || which gpg || gpg`
GPGID=${GPGID:-FE1BC737F9F323D732AA26330620266BE5AFF294}
GITBRANCH=""
TAGS_TARGET="TAGS SCHEMETAGS"
LIBSCM="\${REVISION}"
default_source_dir="\$prefix/src"
default_config_dir="\$prefix/etc/kno"
default_buildopts_dir="\$prefix/etc/beingmeta/opts"
local_config="\$prefix/etc/kno"
shared_config="\$prefix/etc/kno"
rundir=$localstatedir/run
logdir=$localstatedir/log
kno_rundir="$rundir/kno"
kno_logdir="$logdir/kno"
daemon_logdir="$kno_logdir/daemons"
servlet_logdir="$kno_logdir/servlets"
daemon_rundir="$kno_rundir/daemons"
servlet_rundir="$kno_rundir/servlets"
default_bugjar="$kno_logdir/bugjar"
share_dir=""
data_dir=""
archflags=""
with_ziptools_default="yes"
with_mysql_default="yes"
with_crypto_default="yes"
with_imagick_default="yes"
use_dtblock_default="yes"
default_bootsystem="none"
BOOTSETUP="none"
CFLAGS="-DLIBU8_SOURCE -DKNO_SOURCE -fPIC -Wno-unused-label"
PKG_CFLAGS="-fPIC"
OPTFLAGS=
XOPTFLAGS=
CLANG=no
MALLOC_FLAGS="-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free"
MYSQL_CFLAGS=
MYSQL_LDFLAGS=
IMAGICK_CFLAGS=
IMAGICK_LDFLAGS=
LIBZIP_CFLAGS=
LIBZIP_LDFLAGS=
PYTHON2_CFLAGS=
PYTHON2_LDFLAGS=
PYTHON3_CFLAGS=
PYTHON3_LDFLAGS=
DEFAULTISASYNC=0
USEDTBLOCK=0
MSG=echo
debugcflags=" -Og -g3 -ggdb3 -fno-inline -fno-omit-frame-pointer "
debugldflags=""
debuglibs=""
profilecflags=" -O0 -g3 -ggdb3 -fno-inline -fno-omit-frame-pointer "
profileldflags=""
profiledefines=""
manually_disabled=""
auto_disabled=""
force_enabled=""
missing=""

test_env=
memtest_env="MALLOC_CHECK_=3 "
leaktester_env=""
TESTER=./runtest
MEMTESTER=./memtest
LEAKTESTER=./leaktest
MEMTEST_CMD=
LEAKTEST_CMD=
test_config="TESTSIZE=64"
memtest_config=
leaktester_config=

test_size=512
small_test_size=64

use_duma_lib="no"
custom_malloc=""
using_malloc=""
default_malloc=${KNO_DEFAULT_MALLOC:-libc}
gperftools=""
asan=""
static_libgcc="no"
CODENAME="beingmeta"
RELSTATUS="stable"

gperftools_default="yes"

if which lsb_release 2>&1 > /dev/null; then
   CODENAME=`lsb_release -cs || echo beingmeta`
fi

if (test "$rpath_default" = "NONE") || (test "$rpath_default" = ""); then
   rpath_default=$prefix;
fi

AC_CONFIG_AUX_DIR(etc)
AC_CONFIG_HEADER(include/kno/knosource.h include/kno/config.h)

AC_SUBST(DEFAULTMAKETARGET)
AC_SUBST(install_dir_opts)
AC_SUBST(install_file_opts)
AC_SUBST(install_exe_opts)
AC_SUBST(shared_suffix)
AC_SUBST(vshared_suffix)
AC_SUBST(mshared_suffix)
AC_SUBST(mmshared_suffix)
AC_SUBST(dynamic_cflag)
AC_SUBST(OPTFLAGS)
AC_SUBST(XOPTFLAGS)
AC_SUBST(rpath)
AC_SUBST(curlconfig)
AC_SUBST(calltracko)
AC_SUBST(curlo)
AC_SUBST(ldnso)
AC_SUBST(exifo)
AC_SUBST(ipevalo)
AC_SUBST(ipevalopso)
AC_SUBST(suffix)
AC_SUBST(xstatic_exe_libs)
AC_SUBST(xstatic_scheme_libs)
AC_SUBST(xdynamic_scheme_libs)
AC_SUBST(scheme_builtins_init)
AC_SUBST(systemd_loc)
AC_SUBST(share_dir)
AC_SUBST(unpackage_dir)
AC_SUBST(stdlib_module_dir)
AC_SUBST(installed_module_dir)
AC_SUBST(local_module_dir)
AC_SUBST(default_module_path)
AC_SUBST(config_dir)
AC_SUBST(buildopts_dir)
AC_SUBST(local_config)
AC_SUBST(shared_config)
AC_SUBST(data_dir)
AC_SUBST(source_dir)
AC_SUBST(apache_log_dir)
AC_SUBST(apache_modules_dir)
AC_SUBST(apache_conf_dir)
AC_SUBST(apache_modinfo)
AC_SUBST(webuser)
AC_SUBST(webgroup)
AC_SUBST(wwwinstall)
AC_SUBST(knouser)
AC_SUBST(knouserinstall)
AC_SUBST(admin_group)
AC_SUBST(admininstall)
AC_SUBST(daemonid)
AC_SUBST(daemonidinstall)
AC_SUBST(rundir)
AC_SUBST(logdir)
AC_SUBST(bugjar)
AC_SUBST(kno_rundir)
AC_SUBST(kno_logdir)
AC_SUBST(daemon_rundir)
AC_SUBST(daemon_logdir)
AC_SUBST(daemon_bugjar)
AC_SUBST(servlet_rundir)
AC_SUBST(servlet_logdir)
AC_SUBST(servlet_bugjar)
AC_SUBST(archflags)
AC_SUBST(versionsh)
AC_SUBST(definefileinfo)
AC_SUBST(setup_target)
AC_SUBST(extra_sources)
AC_SUBST(debugcflags)
AC_SUBST(debugldflags)
AC_SUBST(debuglibs)
AC_SUBST(profilecflags)
AC_SUBST(profileldflags)
AC_SUBST(profileldefines)
AC_SUBST(profilellibs)

AC_SUBST(test_env)
AC_SUBST(memtest_env)
AC_SUBST(leaktest_env)
AC_SUBST(TESTER)
AC_SUBST(MEMTESTER)
AC_SUBST(LEAKTESTER)
AC_SUBST(LEAKTEST_CMD)
AC_SUBST(MEMTEST_CMD)
AC_SUBST(test_config)
AC_SUBST(memtest_config)
AC_SUBST(leaktest_config)
AC_SUBST(test_size)
AC_SUBST(small_test_size)

AC_SUBST(TAGS)
AC_SUBST(DREENTRANT)
AC_SUBST(SHARED_LIB_TARGET)
AC_SUBST(SHARED_LIB)
AC_SUBST(SHARED_LIBV)
AC_SUBST(test_env)
AC_SUBST(STATICLDFLAGS)
AC_SUBST(TESTLDFLAGS)
AC_SUBST(TESTLIBS)
AC_SUBST(EXEFLAGS)
AC_SUBST(EXELIBS)
AC_SUBST(RPATHFLAGS)
AC_SUBST(KNO_VERSION)
AC_SUBST(KNO_MAJOR)
AC_SUBST(KNO_MINOR)
AC_SUBST(KNO_RELEASE)
AC_SUBST(KNO_PATCHLEVEL)
AC_SUBST(RPMVERSION)
AC_SUBST(DEB_BUILD_TARGET)
AC_SUBST(DEBVERSION)
AC_SUBST(LIBSOURCE)
AC_SUBST(EXESOURCE)
AC_SUBST(STATICEXE)
AC_SUBST(VERSION)
AC_SUBST(PROFILING)
AC_SUBST(MKSTATIC)
AC_SUBST(CLEAN)
AC_SUBST(I18N)
AC_SUBST(INSTALLI18N)
AC_SUBST(SUDO)
AC_SUBST(DOINSTALL)
AC_SUBST(WWWUSER)
AC_SUBST(EXTMODS)
AC_SUBST(CMODULES)
AC_SUBST(INSTALL_CMODULES)
AC_SUBST(TEST_CMODULES)
AC_SUBST(DATAFILES)
AC_SUBST(BOOTSETUP)
AC_SUBST(PKG_CFLAGS)
AC_SUBST(MALLOC_FLAGS)
AC_SUBST(MYSQL_CFLAGS)
AC_SUBST(MYSQL_LDFLAGS)
AC_SUBST(LIBZIP_CFLAGS)
AC_SUBST(LIBZIP_LDFLAGS)
AC_SUBST(IMAGICK_CFLAGS)
AC_SUBST(IMAGICK_LDFLAGS)
AC_SUBST(DEFAULTISASYNC)
AC_SUBST(DEFAULT_DLOAD_PATH)
AC_SUBST(USEDTBLOCK)
AC_SUBST(MSG)
AC_SUBST(BINDIR)
AC_SUBST(CHOWN)
AC_SUBST(CHMOD)
AC_SUBST(GZIP)
AC_SUBST(DELETEFILE)
AC_SUBST(COPYFILE)
AC_SUBST(TOUCHFILE)
AC_SUBST(MKDIR)
AC_SUBST(CHGRP)
AC_SUBST(APTLY)
AC_SUBST(CODENAME)
AC_SUBST(RELSTATUS)
AC_SUBST(BINPATH)
AC_SUBST(LIBSCM)
AC_SUBST(ODBC_LIBS)
AC_SUBST(SQLITE3_LIBS)
AC_SUBST(ZEROMQ_LIBS)

AC_SUBST(PYTHON2_CFLAGS)
AC_SUBST(PYTHON2_LDFLAGS)
AC_SUBST(PYTHON3_CFLAGS)
AC_SUBST(PYTHON3_LDFLAGS)

AC_ARG_VAR(RPMDIR,"Location for writing built RPMs")
AC_ARG_VAR(RPMFLAGS,"Default flags for building RPMs")
AC_ARG_VAR(GPG,"GPG executable")
AC_SUBST(RPMDIR)
AC_SUBST(RPMFLAGS)
AC_SUBST(GPGID)
AC_SUBST(GITBRANCH)
AC_SUBST(PKGCONFIG_DIR)
AC_SUBST(GPG)

if test -z "${GITBRANCH}"; then
   GITBRANCH=`etc/getbranch`;
fi;

AC_PATH_PROG(binenv,env)
AC_CHECK_PROG(SCRIPT_SHELL,sh,,/bin/sh)
AC_CHECK_PROG(ECHO,echo,,/bin/echo)
AC_CHECK_PROG(MKDIR_DEFAULT,mkdir,,/bin/mkdir)
AC_CHECK_PROG(CHMOD_DEFAULT,chmod,,/bin/chmod)
AC_CHECK_PROG(CHOWN_DEFAULT,chown,,/bin/chown)
AC_CHECK_PROG(CHGRP_DEFAULT,chgrp,,/bin/chgrp)
AC_CHECK_PROG(COPYFILE_DEFAULT,cp,,/bin/cp)
AC_CHECK_PROG(DELETEFILE_DEFAULT,rm,,/bin/rm)
AC_CHECK_PROG(TOUCHFILE_DEFAULT,touch,,/bin/touch)
AC_CHECK_PROG(APTLY,aptly,,/usr/bin/aptly)
AC_PATH_PROG(VALGRIND,valgrind,none)
AC_PATH_PROG(DUMA,duma,none)
AC_PATH_PROG(CLANG,clang,none)
AC_CHECK_PROGS(APXS,/usr/local/bin/apxs2 /usr/local/bin/apxs /usr/bin/apxs2 /usr/bin/apxs /usr/sbin/apxs2 /usr/sbin/apxs apxs2 apxs)
AC_CHECK_PROG(CREATEREPO,createrepo)
AC_CHECK_PROGS(LLVM_SYMBOLIZER,llvm-symbolizer-3.5,llvm-symbolizer-3.4)
AC_PATH_PROG(LLVM_SYMBOLIZER_PATH,$LLVM_SYMBOLIZER)
AC_PATH_PROG(GOOGLE_PPROF,google-pprof)
AC_PATH_PROG(XGETTEXT,xgettext)
AC_PATH_PROG(MSGMERGE,msgmerge)
AC_PATH_PROG(MSGFMT,msgfmt)

AC_CHECK_PROGS(GZIP,"gzip","na")

AC_ARG_WITH(execwrapper,--with-execwrapper program to use when calling exec(),,with_execwrapper=$binenv)
if test "$with_execwrapper" != ""; then
  AC_DEFINE_UNQUOTED(KNO_EXEC_WRAPPER,"$with_execwrapper")
fi

chowning=yes
AC_ARG_WITH(chowning,--with/out-chowning,chowning=yes,chowning=no)

AC_CHECK_PROGS(etags,etags,"na")
if test $etags = "na"; then
   TAGS_TARGET="NOTAGS"
fi

AC_CHECK_PROGS(git,"git","na")

AC_CHECK_PROGS(pkg_config,pkg-config /opt/local/bin/pkg-config  /opt/bin/pkg-config /usr/local/bin/pkg-config ,"na")

AC_ARG_WITH(mkdir,--with-mkdir command,with_mkdir=$withval,with_mkdir=$MKDIR_DEFAULT)
MKDIR=$with_mkdir
AC_ARG_WITH(chmod,--with-chmod command,with_chmod=$withval,with_chmod=$CHMOD_DEFAULT)
CHMOD=$with_chmod
AC_ARG_WITH(chown,--with-chown command,with_chown=$withval,with_chown=$CHOWN_DEFAULT)
CHOWN=$with_chown
AC_ARG_WITH(chgrp,--with-chgrp command,with_chgrp=$withval,with_chgrp=$CHGRP_DEFAULT)
CHGRP=$with_chgrp
AC_ARG_WITH(deletefile,--with-deletefile command,with_deletefile=$withval,with_deletefile=$DELETEFILE_DEFAULT)
DELETEFILE=$with_deletefile
AC_ARG_WITH(copyfile,--with-copyfile command,with_copyfile=$withval,with_copyfile=$COPYFILE_DEFAULT)
COPYFILE=$with_copyfile
AC_ARG_WITH(touchfile,--with-touchfile command,with_touchfile=$withval,with_touchfile=$TOUCHFILE_DEFAULT)
TOUCHFILE=$with_touchfile
AC_ARG_WITH(apxs,--with-apxs command,with_apxs=$withval,with_apxs=$APXS)
APXS=$with_apxs

AC_ARG_WITH(aptly,--with-aptly command,with_aptly=$withval)
if test ! -z "${with_aptly}"; then
   case $with_aptly in
   /*) APTLY=$with_aptly;;
   -*) APTLY="${APTLY} $with_aptly";;
   esac;
fi;

AC_ARG_WITH(default-target,--with-default-target default-make-target,with_default_target=$withval,with_default_target=update)
DEFAULTMAKETARGET=$with_default_target

AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AC_PROG_CC([${CC} cc gcc clang])
AC_USE_SYSTEM_EXTENSIONS
AC_AIX

AC_PROG_INSTALL
AC_PROG_RANLIB

CFLAGS="-I$prefix/include/$host/ $CFLAGS"
PKG_CFLAGS="-I$prefix/include/$host/ $PKG_CFLAGS"

AC_DEFINE_UNQUOTED(KNO_VERSION,"$KNO_VERSION")
AC_DEFINE_UNQUOTED(KNO_MAJOR_VERSION,$KNO_MAJOR)
AC_DEFINE_UNQUOTED(KNO_MINOR_VERSION,$KNO_MINOR)
AC_DEFINE_UNQUOTED(KNO_RELEASE_VERSION,$KNO_RELEASE)

AC_ARG_WITH(suffix,--with-suffix string,,with_suffix="")
suffix=$with_suffix

if test ! -z "${KNO_PATCHLEVEL}"; then
   echo "Preset patchlevel ${KNO_PATCHLEVEL}";
elif test -d .git; then
   KNO_PATCHLEVEL=$(u8_gitpatchcount ./etc/base_version);
elif test -f VERSION; then
   KNO_PATCHLEVEL=$(cat VERSION | cut -d'-' -f2);
else KNO_PATCHLEVEL=unknown;
fi;
   

if test -f .ipeval; then
  enable_ipeval_default="yes;
else enable_ipeval_default="no;
fi

AC_ARG_WITH(optinclude,--with-optinclude Use optional include directory,,with_optinclude="none")
if test "$with_optinclude" != "none"; then
  CPPFLAGS="$CPPFLAGS -I$with_optinclude"
  CFLAGS="$CFLAGS -I$with_optinclude"
fi

AC_ARG_WITH(optlib,--with-optlib Use optional library directory,,with_optlib="none")
if test "$with_optlib" != "none"; then
  LDFLAGS="$LDFLAGS -L$with_optlib"
fi

AC_ARG_WITH(sourcedir,--with-sourcedir source directory,source_dir=$withval,source_dir=$prefix/src)

AC_ARG_WITH(extra_sources,--with-extra_sources Configure extra source directories for gdb,,with_extra_sources="none")
if test "$with_extra_sources" != "none"; then
   extra_sources=${with_extra_sources};
fi

AC_ARG_ENABLE(stripping,--enable-stripping strip libraries and executables before installing,,enable_stripping="no")
if test "$enable_stripping" != "no"; then
   LIBSOURCE=lib/stripped
   EXESOURCE=exe/stripped
   STATICEXE=static/stripped
fi

AC_ARG_ENABLE(debsign,--dis[en]able-debian package signing,,enable_debsign="yes")
if test "$enable_debsign" == "no"; then
   DEB_BUILD_TARGET=dist/debs.built;
fi;

AC_ARG_WITH(optdir,--with-optdir Use optional header/lib,,with_optdir="/opt/local")
if test -d $with_optdir/include; then
  CPPFLAGS="$CPPFLAGS -I$with_optdir/include"
  CFLAGS="$CFLAGS -I$with_optdir/include"
  APXSFLAGS="$APXSFLAGS -I$with_optdir/include"
fi
if test -d $with_optdir/lib; then
  LDFLAGS="$LDFLAGS -L$with_optdir/lib"
fi
if test -d $with_optdir/bin; then
  PATH="$PATH:$with_optdir/bin"
fi

dnl Specify an additional library
AC_ARG_WITH(extlib,
[  --with-extlib[[=DIR]]       use additional lib dir],[
  if test "$withval" != "no" -a "$withval" != "yes"; then
    LDFLAGS="${LDFLAGS} -l$withval"
  fi
])

AC_ARG_WITH(rpath,--with-rpath dir,rpath=$withval,rpath=$rpath_default)

if test "$rpath" != "none" && test "$rpath" != "/usr" && test "$rpath" != "/usr/local"; then
  EXEFLAGS=" $EXEFLAGS \$(RPATHFLAGS) "
fi

AC_ARG_WITH(pkgconfig_dir,--with-pkgconfig-dir pathname,,with_pkgconfig_dir=$prefix/share/pkgconfig)
PKGCONFIG_DIR=${with_pkgconfig_dir}
AC_DEFINE_UNQUOTED(KNO_PKGCONFIG_DIR,"$with_pkgconfig_dir")


### Checking the compiler

AC_CHECK_DECL(__clang__,CLANG=yes)

if test "${CLANG}" = "yes"; then
   CFLAGS="${CFLAGS} -fno-strict-aliasing -g3 -ggdb"
   OPTFLAGS="-O2"
   XOPTFLAGS="-O3 -funroll-loops"
elif test "${GCC}" = "yes"; then
   CFLAGS="${CFLAGS} -fno-strict-aliasing -g3 -ggdb"
   OPTFLAGS="-O2"
   XOPTFLAGS="-O3 -funroll-loops"
   gcc_version=`gcc -dumpversion`;
   dynamic_cflag="-rdynamic";
fi;

m4_include([etc/ax_pthread.m4])
m4_include([etc/ax_atomics.m4])
m4_include([etc/ax_check_link_flag.m4])
m4_include([etc/ax_python.m4])
m4_include([etc/ax_gcov.m4])

AC_DEFINE(KNO_WORDS_ARE_ALIGNED,1)
AC_TYPE_UID_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_C_INLINE
AC_C_BIGENDIAN
AC_CHECK_SIZEOF(short,2)
AC_CHECK_SIZEOF(int,4)
AC_CHECK_SIZEOF(long,4)
AC_CHECK_SIZEOF(long long,8)
AC_CHECK_SIZEOF(void *,8)
AC_CHECK_SIZEOF(time_t,8)
AC_CHECK_TYPES(uchar)

case $host_os in
  darwin*)
#   MKSTATIC="libtool -static "
   dynamic_cflag="-dynamic"
   shared_suffix="dylib"
   vshared_suffix=${KNO_VERSION}.dylib
   mshared_suffix=${KNO_MAJOR}.dylib
   mmshared_suffix=${KNO_MAJOR}.${KNO_MINOR}.dylib
   APXS="apxs -Wc,'-arch x86_64' -Wl,'-arch x86_64'  -a $APXSFLAGS"
   CFLAGS="-D_DARWIN_C_SOURCE $CFLAGS"
   PKG_CFLAGS="-D_DARWIN_C_SOURCE $PKG_CFLAGS"
   RPATHFLAGS=" -rpath \$(RPATH) "
   STATICLDFLAGS=" -lc++"
   ifmacosx=""
   setup_target="setup-osx"
   gperftools_default="no"
   ;;
  *)
   MKSTATIC="ld -r --build-id "
   ;;
esac  

# Now that we have shared_suffix defined, set some things up

CMODULES="lib/kno/testcapi.${shared_suffix} ${CMODULES}"
INSTALL_CMODULES="install-testcapi ${INSTALL_CMODULES}"

AX_CHECK_LINK_FLAG("-static-libgcc",static_libgcc="yes")

if test "${static_libgcc}" == "yes"; then
   LDFLAGS="-static-libgcc -Wl,--as-needed ${LDFLAGS}"
fi

AC_ARG_WITH(fileinfo,--with/out-fileinfo record fileinfo,,with_fileinfo="yes")
if test "$with_fileinfo" = "yes" ; then
   definefileinfo="-D_FILEINFO=\"\\\"\`./fileinfo ./$< .\`\\\"\""
   versionsh="include/kno/versions.h"
fi

AC_ARG_WITH(stackcheck,--with-stackcheck Build with native stack checking,with_stackcheck=$withval,with_stackcheck="yes")
if ! test "${with_stackcheck}" = "no"; then
   AC_DEFINE_UNQUOTED(KNO_STACKCHECK,1)
fi

# profiling

dnl This compiles a version to be used with the GNU profiling tools
AC_ARG_ENABLE(cprofiling,--en/disable-cprofiling compile to support profiling and coverage tools,,enable_cprofiling="no")

if test -z "$enable_cprofiling" || test "$enable_cprofiling" = "no"; then
  AC_DEFINE(KNO_PROFILING,0)
  PROFILING=""
elif test $enable_cprofiling = "yes"; then
  AC_DEFINE(KNO_PROFILING,1)
  PROFILING="-O0 -fno-inline"
elif test $enable_cprofiling = "deep" || test $enable_cprofiling = "2" ; then
  AC_DEFINE(KNO_PROFILING,2)
  PROFILING="-O0 -fno-inline"
elif test $enable_cprofiling = "extreme" || test $enable_cprofiling = "3" ; then
  AC_DEFINE(KNO_PROFILING,3)
  PROFILING="-O0 -fno-inline"
else 
  AC_DEFINE(KNO_PROFILING,0)
  PROFILING=""
fi

dnl This compiles a version to be used with the GNU profiling tools
AC_ARG_WITH(gnuprof,--with/out-gnuprof compile to generate GNU profiling information,,with_gnuprof="no")

if test "$with_gnuprof" = "yes"; then
   if test -z "$PROFILING" ; then
     AC_DEFINE(KNO_PROFILING,2)
     PROFILING="-O0 -pg -fno-inline -fprofile-arcs -ftest-coverage"
    else
      PROFILING="${PROFILING} -pg"
    fi;
fi;

### GNU Coverage checking

AC_TDD_GCOV
AC_SUBST(GCOV_CFLAGS)
AC_SUBST(GCOV_LDFLAGS)

if test "${use_gcov}" == "yes"; then
  AC_DEFINE(KNO_PROFILING,2)
  PROFILING="${PROFILING} -fprofile-arcs -ftest-coverage"
  OPTFLAGS=
  XOPTFLAGS=
fi;

### Check some compiler fancies

dnl ***********************************
dnl *** Checks for working __builtin_expect ***
dnl ***********************************
AC_MSG_CHECKING(for working __builtin_expect)
AC_TRY_COMPILE([],[
	     int main (int argc,char *argv[]) {
                if (__builtin_expect((argc>2),1)) return 0;
                else return 1;
		}
	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_BUILTIN_EXPECT)],
	       [AC_MSG_RESULT(no)])


dnl ***********************************
dnl *** Checks for working __builtin_prefetch ***
dnl ***********************************
AC_MSG_CHECKING(for working __builtin_prefetch)
AC_TRY_COMPILE([],[
	     static int data[]={0,1,2,3};

	     int main (int argc,char *argv[]) {
                __builtin_prefetch(&data[2],0,3);
		return 0;
		}
	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_BUILTIN_PREFETCH)],
	       [AC_MSG_RESULT(no)])


dnl ***********************************
dnl *** Checks for working constructor attributes ***
dnl ***********************************
AC_MSG_CHECKING(for working __constructor_attributes)
AC_TRY_COMPILE([],[
             int tmpinit(void) __attribute__ ((constructor));

	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_CONSTRUCTOR_ATTRIBUTES)],
	       [AC_MSG_RESULT(no)])

### What flavor of malloc() to use

AC_ARG_WITH(duma,--with-duma Build with Detect Unintended Memory Access,with_duma=$withval,with_duma="no")
if test "$with_duma" != "no"; then
   default_malloc="libc";
fi

WITHMALLOC_OPTS="libc builtin jemalloc asan tcmalloc dmalloc list"

AC_ARG_WITH(malloc,--with-malloc Use custom malloc: e.g. $WITHMALLOC_OPTS,with_malloc=$withval,with_malloc=$default_malloc)

if test "$with_malloc" = "jemalloc"; then
  AC_CHECK_LIB(jemalloc,malloc,EXELIBS="-ljemalloc $EXELIBS")
elif test "$with_malloc" = "dmalloc"; then
  AC_CHECK_LIB(dmalloc,malloc,EXELIBS="-ldmalloc $EXELIBS")
fi

AC_MSG_CHECKING(malloc library)

if test -z "$with_malloc" || 
   test "$with_malloc" = "libc" ||
   test "$with_malloc" = "builtin" ; then
   AC_MSG_RESULT(libc default)
   custom_malloc="";
   using_malloc="libc"
elif test "$with_duma" != "no"; then
   AC_MSG_ERROR(DUMA can only be used with libc malloc)
   exit;
elif test "$with_malloc" = "tcmalloc"; then
  gperftools="yes";
  custom_malloc="tcmalloc"
  using_malloc="tcmalloc"
  AC_MSG_RESULT(tcmalloc)
elif test "$with_malloc" = "gperftools"; then
  gperftools="profile";
  custom_malloc="tcmalloc"
  using_malloc="gperftools"
  AC_MSG_RESULT(tcmalloc profiling)
elif test "$with_malloc" = "tcmalloc_debug"; then
  gperftools="debug";
  custom_malloc="tcmalloc_debug"
  using_malloc="tcmalloc_debug"
  AC_MSG_RESULT(tcmalloc debug)
elif test "$with_malloc" = "tcmalloc_minimal"; then
  gperftools="minimal";
  custom_malloc="tcmalloc_minimal"
  using_malloc="tcmalloc_minimal"
  AC_MSG_RESULT(tcmalloc minimal)
elif test "$with_malloc" = "jemalloc"; then
  if test "${ac_cv_lib_jemalloc_malloc}" = "yes"; then
    custom_malloc="jemalloc"
    using_malloc="jemalloc"
    AC_MSG_RESULT(jemalloc)
    memtest_env="junk:true,tcache:false"
    leaktest_env="prof_leak:true,lg_prof_sample:0,prof_final:true"
  else
    AC_MSG_ERROR(jemalloc can't be found)
    exit;
  fi;
elif test "$with_malloc" = "dmalloc"; then
  if test "${ac_cv_lib_dmalloc_malloc}" = "yes"; then
    custom_malloc="dmalloc"
    using_malloc="dmalloc"
    AC_MSG_RESULT(dmalloc)
  else
    AC_MSG_ERROR(dmalloc can't be found)
    exit;
  fi;
elif test "$with_malloc" = "asan"; then
   CFLAGS="-fsanitize=address -fno-omit-frame-pointer -O0 -g $CFLAGS"
   LDFLAGS="-g -fsanitize=address $LDFLAGS"
   build_static=""
   test_static=""
   install_static=""
   custom_malloc="asan"
   using_malloc="asan"
   test_env="MEMCHECKING=yes ASAN_SYMBOLIZER_PATH=$LLVM_SYMBOLIZER_PATH ASAN_OPTIONS=malloc_context_size=20 LSAN_OPTIONS=report_objects=1 $test_env";
   leaktester_env="ASAN_OPTIONS=malloc_context_size=20 LSAN_OPTIONS=report_objects=1 ${leaktester_env}";
   asan="yes"
   test_size=64
   AC_MSG_RESULT(ASAN)
elif test "$with_malloc" = "asan_noleak"; then
   CFLAGS="-fsanitize=address -fno-omit-frame-pointer -O0 -g $CFLAGS"
   LDFLAGS="-g -fsanitize=address $LDFLAGS"
   build_static=""
   test_static=""
   install_static=""
   custom_malloc="asan"
   using_malloc="asan"
   test_env="MEMCHECKING=yes ASAN_SYMBOLIZER_PATH=$LLVM_SYMBOLIZER_PATH ASAN_OPTIONS=detect_leaks=0:malloc_context_size=20 $test_env";
   leaktester_env="ASAN_OPTIONS=malloc_context_size=20:detect_leaks=1 LSAN_OPTIONS=report_objects=1 ${leaktester_env}";
   asan="yes"
   test_size=64
   AC_MSG_RESULT(ASAN)
elif test "$with_malloc" = "asan_thread"; then
   CFLAGS="-fsanitize=thread -g -fno-omit-frame-pointer $CFLAGS"
   LDFLAGS="-g -fsanitize=thread -fno-omit-frame-pointer $LDFLAGS"
   build_static=""
   test_static=""
   install_static=""
   custom_malloc="asan_thread"
   using_malloc="asan_thread"
   test_env="MEMCHECKING=yes MSAN_SYMBOLIZER_PATH=$LLVM_SYMBOLIZER_PATH $test_env";
   asan="thread"
   AC_MSG_RESULT(ASAN thread)
else
   AC_MSG_ERROR(Valid values for malloc are libc tcmalloc jemalloc gperftools dmalloc tcmalloc_debug asan asan_thread)
   exit 1;
fi

if test ! -z "$using_malloc"; then
   AC_DEFINE_UNQUOTED(KNO_DEFAULT_MALLOC,"$using_malloc")
fi;

#if test ! -z "$asan" && test "$CLANG" = "none"; then
#   AC_MSG_ERROR(No clang program to use with ASAN)
#fi;

if test -z "$gperftools" || test "$gperftools" = "no"; then
   ${ECHO} -n;
else
  AC_CHECK_HEADERS(gperftools/profiler.h)
  AC_CHECK_HEADERS(gperftools/heap-profiler.h)
  AC_CHECK_HEADERS(gperftools/heap-checker.h)
  AC_CHECK_HEADERS(gperftools/malloc_extension_c.h)
  if test "$gperftools" = "debug"; then
    AC_CHECK_LIB(tcmalloc_debug,malloc,EXELIBS="-ltcmalloc_debug $EXELIBS")
    AC_CHECK_LIB(profiler,ProfilerStart,EXELIBS="-lprofiler $EXELIBS")
  elif test "$gperftools" = "profile"; then
    AC_CHECK_LIB(tcmalloc,malloc,EXELIBS="-ltcmalloc $EXELIBS")
    AC_CHECK_LIB(profiler,ProfilerStart,EXELIBS="-lprofiler $EXELIBS")
    CMODULES="lib/kno/gperftools.${shared_suffix} ${CMODULES}"
    INSTALL_CMODULES="install-gperftools ${INSTALL_CMODULES}"
  elif test "$gperftools" = "minimal"; then
    AC_CHECK_LIB(tcmalloc_minimal,malloc,EXELIBS="-ltcmalloc_minimal $EXELIBS")
  else
    AC_CHECK_LIB(tcmalloc,malloc)
  fi;
  if test "${ac_cv_lib_tcmalloc_malloc}" == "yes" || \
     test "${ac_cv_lib_tcmalloc_debug_malloc}" == "yes" || \
     test "${ac_cv_lib_tcmalloc_and_profiler_malloc}" == "yes"; then
    custom_malloc="tcmalloc";
    CFLAGS="${CFLAGS} ";
    leaktest_env="HEAPCHECK=normal PPROF_PATH=${GOOGLE_PPROF}";
    memtest_env="TCMALLOC_DEBUG=5";
  else
    echo "No tcmalloc available";
    custom_malloc="";
    gperftools="no";
  fi;
fi

if test "$with_duma" != "no" && test "$custom_malloc" != ""; then
  echo "Can't use DUMA with other custom MALLOC libraries ($custom_malloc)";
  with_duma="no";
elif test "$with_duma" != "no" && test "$DUMA" = "none"; then
  echo "No DUMA executable";
  with_duma="no";
elif test "$with_duma" != "no"; then
  AC_CHECK_LIB(duma,malloc)
  AC_CHECK_HEADERS(duma.h)
  test_env="DUMA_ALIGNMENT=4 $test_env";
  custom_malloc="DUMA";
fi

if test "${VALGRIND}" != "none" && test -z "${custom_malloc}"; then
  MEMTEST_CMD="KNO_INVALGRIND=yes valgrind --tool=memcheck --num-callers=24"
  LEAKTESTER="KNO_INVALGRIND=yes valgrind --tool=memcheck --leak-check=full --num-callers=24"
fi

# OpenSSL checks
AC_CHECK_FUNCS(CRYPTO_set_locking_callback)

### Regular expressions library

regex_included=no

AC_CHECK_HEADER(regex.h,have_regex_h="yes",have_regex_h="no")
if (test "$have_regex_h" = "yes"); then
  AC_CHECK_FUNC(regcomp,have_regcomp="yes",have_regcomp="no")
  regex_included=yes
fi
if (test "$have_regex_h" = "yes") && (test "$have_regcomp" = "no"); then
  AC_CHECK_LIB(regex,regcomp,have_regcomp="yes",have_regcomp="no")
  regex_included=yes
fi

if test ! "${regex_included}" = "yes"; then
   missing="regex ${missing}";
fi   

# IPEVAL configuration (disabled by default, patented)

dnl This enables the ipeval implementation within Kno
AC_ARG_ENABLE(ipeval,--en/disable-ipeval Enable IPEVAL,,enable_ipeval=$enable_ipeval_default)
if test $enable_ipeval = "yes"; then
  ipevalo=src/storage/ipeval.o
  ipevalopso=src/eval/ipevalops.o
  AC_DEFINE(KNO_IPEVAL_ENABLED,1)
else
  AC_DEFINE(KNO_IPEVAL_ENABLED,0)
fi

dnl This forces the use of a global lock for ipeval, which can reduce
dnl  parallellism  but may help track down subtle bugs
AC_ARG_WITH(global_ipeval,--with/out-global-ipeval Use global lock and variables for ipeval,,with_global_ipeval=$global_ipeval_default)
if test $with_global_ipeval = "yes"; then
  AC_DEFINE(KNO_GLOBAL_IPEVAL,1)
else
  AC_DEFINE(KNO_GLOBAL_IPEVAL,0)
fi

## Build CURL cmodule

dnl Build with primitives accessing libcurl, if it is available
AC_CHECK_LIB(curl,curl_easy_init,curl_default="yes")
AC_ARG_WITH(curl,--with/out-curl Use libcurl in webtools,,with_curl=$curl_default)
if test $with_curl = "yes"; then
  AC_CHECK_LIB(curl,curl_easy_init)
  AC_CHECK_HEADERS(curl/curl.h)
  AC_DEFINE(KNO_WITH_CURL,1)
  curlo="src/web/curl.o"
  curlconfig="\`curl-config --libs\`"
else
  AC_DEFINE(KNO_WITH_CURL,0)
  missing="curl ${missing}"
fi

## Providing DNS functions in webtools

dnl Build with primitives accessing DNS resolution functions, if available
AC_CHECK_HEADERS(resolv.h)
AC_CHECK_HEADERS(arpa/nameser.h)
AC_CHECK_HEADERS(netinet/in.h)
AC_CHECK_HEADERS(netdb.h)
AC_CHECK_LIB(ldns,ldns_resolver_query,dns_default="yes")
AC_ARG_WITH(dns,--with/out-dns Use dns in web,,with_dns=$dns_default)
if test "$with_dns" = "yes"; then
  AC_CHECK_LIB(ldns,ldns_resolver_query)
  AC_DEFINE(KNO_WITH_DNS,1)
  ldnso="src/web/ldns.o"
else
  AC_DEFINE(KNO_WITH_DNS,0)
  missing="ldns ${missing}"
fi

# For UUID functions
AC_CHECK_LIB(uuid,uuid_generate_time)
AC_CHECK_FUNCS(uuid_generate_time)

## Large files

dnl Compile for access to large files, essentially defining off_t as 64 bits.
AC_ARG_ENABLE(largefiles,--en/disable-largefiles Enable use of large (> 2GB) file support,,enable_largefiles=yes)
if test $enable_largefiles = "yes"; then
  AC_DEFINE(KNO_LARGEFILES_ENABLED,1)
  AC_SYS_LARGEFILE
  AC_CHECK_SIZEOF(off_t)
  AC_CHECK_SIZEOF(size_t)
else
  AC_DEFINE(KNO_LARGEFILES_ENABLED,0)
  AC_CHECK_SIZEOF(off_t)
  AC_CHECK_SIZEOF(size_t)

fi

# libu8 configuration

AC_CHECK_LIB(u8,u8_raise)
AC_CHECK_LIB(u8,u8_open_xinput)
if test "$ac_cv_lib_u8_u8_open_xinput" != "yes"; then
      AC_CHECK_LIB(u8io,u8_open_xinput)
fi
AC_CHECK_LIB(u8data,u8_init_chardata_c)
dnl Check for cryptic in in libu8
AC_CHECK_FUNCS(u8_cryptic,with_crypto_default="yes")
AC_ARG_WITH(crypto,--with/out-crypto Compile the crypto module,,with_crypto=$with_crypto_default)
if test $with_crypto = "yes"; then
  CMODULES="lib/kno/crypto.$shared_suffix ${CMODULES}"
  INSTALL_CMODULES="install-crypto ${INSTALL_CMODULES}"
  TEST_CMODULES="crypto ${TEST_CMODULES}"
fi

dnl This integrates the webtools module into executables by default; otherwise
dnl  it is built and installed as a plugin library
AC_ARG_ENABLE(htmldump,--en/disable-htmldump write HTML backtraces,,enable_htmldump="no")
if test $enable_htmldump = "yes"; then
  scheme_builtins_init="$scheme_builtins_init; kno_init_webtools()"
  xdynamic_scheme_libs="$xdynamic_scheme_libs -lknowebtools${suffix}"
  xstatic_exe_libs="lib/libknowebtools${suffix}.a $xstatic_exe_libs"
  xstatic_scheme_libs=" ../../lib/libknowebtools${suffix}.a $xstatic_scheme_libs"
  AC_DEFINE(KNO_HTMLDUMP_ENABLED,1)
else
  AC_DEFINE(KNO_HTMLDUMP_ENABLED,0)
fi

### Object/index database prefetching

dnl This is used to disable the simplest automatic database prefetches.
dnl This is typically used to configure for benchmarking prefetch mechanisms
dnl against the base case of no prefetches at all.
AC_ARG_ENABLE(prefetching,--en/disable-prefetching make frame operations prefetch when they can,,enable_prefetching="yes")
if test $enable_prefetching = "yes"; then
  AC_DEFINE(KNO_PREFETCHING_ENABLED,1)
else
  AC_DEFINE(KNO_PREFETCHING_ENABLED,0)
fi

### Installation configuration

AC_ARG_WITH(codename,--with-codename for distribution,with_codename=$withval,with_codename=$CODENAME)
CODENAME=$with_codename

AC_ARG_WITH(relstatus,--with-relstatus e.g. stable or testing,with_relstatus=$withval)

if test ! -z "${with_relstatus}"; then
   RELSTATUS=$with_relstatus
fi;

AC_CHECK_PROGS(buildrpm,"lsb-rpmbuild","rpmbuild","lsb-rpm","na")
AC_CHECK_PROGS(rpm,"lsb-rpm","rpm","na")

### System users, groups, and paths for Kno

AC_CHECK_GROUP(default_admin_group,kno _kno adm admin)
AC_CHECK_USER(default_daemonid,knod _knod knodaemon _knodaemon daemon nobody)
AC_CHECK_USER(default_daemon,xdaemon knod _knod knodaemon _knodaemon daemon nobody)
AC_CHECK_USER(default_webuser,www apache httpd www-data _www)
AC_CHECK_USER(default_webgroup,www apache httpd www-data _www)

dnl Specifies an admin group; files written by installation scripts will be
dnl owned by this group and the group will have full rwX access.
AC_ARG_WITH(admin_group,--with-admin-group Install files with admin group and group write permissions,,with_admin_group=$default_admin_group)
if test "$with_admin_group" != "none"; then
   if test "$chowning" = "yes"; then
     install_dir_opts="-g $with_admin_group -m 0775 $install_dir_opts"
     install_file_opts="-g $with_admin_group $install_file_opts"
     install_exe_opts="-g $with_admin_group $install_exe_opts"
     admininstall="-g $with_admin_group"
  fi;
  admin_group=$with_admin_group
else
  install_dir_opts="$install_dir_opts"
  install_file_opts="$install_file_opts"
  install_exe_opts="$install_exe_opts"
  admin_group=$default_admin_group
  admininstall=""
fi

AC_DEFINE_UNQUOTED(KNO_ADMIN_GROUP,"$admin_group")

dnl Specifies a daemon identity user/group for managed Kno servers
AC_ARG_WITH(daemonid,--with-daemonid The identity (user) for Kno servers to run as,,with_daemonid=$default_daemonid)
if test "$with_daemonid" != "none"; then
   knouser=$with_daemonid
   if test "$chowning" = "yes"; then
     knouserinstall="-o $knouser";
   fi;
else
   knouserinstall=""
   knouser=$default_daemonid
fi

AC_DEFINE_UNQUOTED(KNO_DAEMON,"$knoaemon")

dnl Specifies where server state files should live
AC_ARG_WITH(rundir,--with-rundir The default location for server state files,,with_rundir="$kno_rundir")
kno_rundir=$with_rundir
AC_DEFINE_UNQUOTED(KNO_RUN_DIR,"$kno_rundir")

dnl Specifies where log files should live
AC_ARG_WITH(logdir,--with-logdir The default location for log files,,with_logdir="$kno_logdir")
kno_logdir=$with_logdir

dnl Specifies where the default bugjar location
AC_ARG_WITH(bugjar,--with-bugjar The default location for bugjar reports,,with_bugjar="$default_bugjar")
bugjar=$with_bugjar

AC_DEFINE_UNQUOTED(KNO_LOG_DIR,"$kno_logdir")
AC_DEFINE_UNQUOTED(KNO_DAEMON_LOG_DIR,"$daemon_logdir")
AC_DEFINE_UNQUOTED(KNO_SERVLET_LOG_DIR,"$servlet_logdir")
AC_DEFINE_UNQUOTED(KNO_DAEMON_RUN_DIR,"$daemon_rundir")
AC_DEFINE_UNQUOTED(KNO_SERVLET_RUN_DIR,"$daemon_rundir")
AC_DEFINE_UNQUOTED(KNO_BUGJAR_ROOT,"$bugjar")

### Paths

### Users and Groups

dnl Specifies the user which the Web server runs as
dnl This is used to set up the directories used by knocgi
AC_ARG_WITH(webuser,--with-webuser The user for the webserver,,with_webuser=$default_webuser)
if test "$with_webuser" != "none"; then
   webuser=$with_webuser
   if test "$chowning" = "yes"; then wwwinstall="-o $webuser"; fi
else
   webuser=$default_webuser
   wwwinstall=""
fi

dnl Specifies the group which the Web server runs as
dnl This is used to set up the directories used by knocgi
AC_ARG_WITH(webgroup,--with-webgroup The group for the webserver,,with_webgroup=$default_webgroup)
if test "$with_webgroup" != "none" && test "$chowning" = "yes"; then
   webgroup=$with_webgroup
   if test "$chowning" = "yes"; then wwwinstall="$wwwinstall -g $webgroup"; fi
else
   webgroup=$default_webgroup
   wwwinstall=""
fi

AC_DEFINE_UNQUOTED(KNO_WEBUSER,"$webuser")
AC_DEFINE_UNQUOTED(KNO_WEBGROUP,"$webgroup")

### Apache

AC_MSG_CHECKING(apache log directory)
if test -d ${apache_conf_dir}/log; then
   if test -h ${apache_conf_dir}/log; then
     apache_log_dir=$(readlink ${apache_conf_dir}/log);
   else apache_log_dir=${apache_conf_dir}/log;
   fi
elif test -d ${apache_conf_dir}/logs; then
   if test -h ${apache_conf_dir}/logs; then
     apache_log_dir=$(readlink ${apache_conf_dir}/logs);
   else apache_log_dir=${apache_conf_dir}/logs;
   fi
elif test -d ${local_state_dir}/logs/apache; then
    apache_log_dir=${local_state_dir}/logs/apache;
elif test -d ${local_state_dir}/logs/apache2; then
    apache_log_dir=${local_state_dir}/logs/apache2;
elif test -d ${local_state_dir}/logs/httpd; then
    apache_log_dir=${local_state_dir}/logs/httpd;
elif test -d ${local_state_dir}/logs/http; then
    apache_log_dir=${local_state_dir}/logs/http;
elif test -d /var/log/apache; then
    apache_log_dir=/var/log/apache;
elif test -d /var/log/apache2; then
    apache_log_dir=/var/log/apache2;
elif test -d /var/log/httpd; then
    apache_log_dir=/var/log/httpd;
elif test -d /var/log/http; then
    apache_log_dir=/var/log/http;
elif test -d /usr/log/apache; then
    apache_log_dir=/usr/log/apache;
elif test -d /usr/log/apache2; then
     apache_log_dir=/usr/log/apache2;
elif test -d /usr/log/httpd; then
    apache_log_dir=/usr/log/httpd;
elif test -d /usr/log/http; then
    apache_log_dir=/usr/log/http;
else
    apache_log_dir=""
fi;

if test -z ${apache_log_dir}; then
 AC_MSG_RESULT(none found, assuming /var/log/apache2/)
 apache_log_dir=/var/log/apache;
else
 AC_MSG_RESULT($apache_log_dir);
fi;

if test ! -z "$APXS"; then
  apache_conf_dir=`$APXS -q SYSCONFDIR`
  apache_modules_dir=`$APXS -q LIBEXECDIR`
  if test -d ${apache_conf_dir}/mods-available; then
    apache_modinfo=${apache_conf_dir}/mods-available;
  elif test -d ${apache_conf_dir}/mods_available; then
    apache_modinfo=${apache_conf_dir}/mods_available;
  elif test -d ${apache_conf_dir}.d; then
    apache_modinfo=${apache_conf_dir}.d;
  elif test -d ${apache_conf_dir}/other; then
    apache_modinfo=${apache_conf_dir}/other;
  else apache_modinfo=$apache_conf_dir;
  fi;
else
  echo "Warning: No apache development environment, can't build mod_knocgi"
fi

dnl Allow overrides of APXS apacheinfo and apachelib
AC_ARG_WITH(apacheinfo,--with-apacheinfo where Apache module info lives,,with_apacheinfo=$apache_modinfo)
AC_ARG_WITH(apachelib,--with-apachelib where Apache extensions live,,with_apachelib=$apache_modules_dir)
apache_modinfo=$with_apacheinfo
apache_modules_dir=$with_apachelib

### Provide the FFI interface

AC_ARG_ENABLE(ffi,--enable-ffi enable the FFI interface,enable_ffi=$enableval,enable_ffi="yes")
if test "${enable_ffi}" == "yes"; then
   AC_CHECK_HEADERS(ffi/ffi.h)
   AC_CHECK_HEADERS(ffi.h)
   AC_CHECK_LIB(ffi,ffi_prep_cif)
fi
if test "${enable_ffi}" != "yes"; then
   manually_disabled="ffi ${manually_disabled}"
elif test ! "${ac_cv_header_ffi_h}" != "yes" &&
     test ! "${ac_cv_header_ffi_ffi_h}" != "yes";  then
   enable_ffi="no";
   auto_disabled="ffi ${auto_disabled}";
elif test "${ac_cv_lib_ffi_ffi_prep_cif}" != "yes"; then
   enable_ffi="no";
   auto_disabled="ffi ${auto_disabled}";
   missing="${missing} ffi";
fi

VSHARED_LOADPATH="${prefix}/lib/kno${suffix}/%.${vshared_suffix}:${prefix}/lib/%.${vshared_suffix}:${prefix}/lib/libkno%.${vshared_suffix}"
MMSHARED_LOADPATH="${prefix}/lib/kno${suffix}/%.${mmshared_suffix}:${prefix}/lib/%.${mmshared_suffix}:${prefix}/lib/libkno%.${mmshared_suffix}"
MSHARED_LOADPATH="${prefix}/lib/kno${suffix}/%.${mshared_suffix}:${prefix}/lib/%.${mshared_suffix}:${prefix}/lib/libkno%.${mshared_suffix}"
SHARED_LOADPATH="${prefix}/lib/kno${suffix}/%.${shared_suffix}:${prefix}/lib/%.${shared_suffix}:${prefix}/lib/libkno%.${shared_suffix}"
LSHARED_LOADPATH="${prefix}/lib/lib%.${vshared_suffix}:${prefix}/lib/lib%.${mmshared_suffix}:${prefix}/lib/lib%.${mshared_suffix}:${prefix}/lib/lib%.${shared_suffix}"
DEFAULT_DLOADPATH="${VSHARED_LOADPATH}:${MMSHARED_LOADPATH}:${MSHARED_LOADPATH}:${SHARED_LOADPATH}"

dnl For specifying default -arch flags for OSX
AC_ARG_WITH(arch,--with/out-arch build with particular --arch flags,,with_arch="")
if test -z $with_arch; then
  archflags=""
elif test $with_arch = "all"; then
  archflags="-arch i386 -arch x86_64 -arch ppc -arch ppc64";
elif test $with_arch = "all86"; then
  archflags="-arch i386 -arch x86_64";
elif test $with_arch = "allppc"; then
  archflags="-arch ppc -arch ppc64";
else archflags=$with_arch
fi

# Check whether to use an architecture specific library directory
dnl if test "${libdir}" != "\${exec_prefix}/lib"; then
dnl   echo checking libdir, using explicit libdir ${libdir};
dnl elif test ${ac_cv_sizeof_long} -eq 4 && test -d ${prefix}/lib32; then
dnl   libdir=${prefix}/lib32;
dnl   echo checking libdir, using 32-bit libdir ${libdir};
dnl elif test ${ac_cv_sizeof_long} -eq 8 && test -d ${prefix}/lib64; then
dnl   libdir=${prefix}/lib64;
dnl   echo checking libdir, using 64-bit libdir ${libdir};
dnl else
dnl   echo checking libdir, using default libdir ${libdir};
dnl fi
dnl LDFLAGS="-L${libdir} ${LDFLAGS}"

dnl Some system headers and members

AC_HEADER_STDC
AC_HEADER_STAT

AC_CHECK_HEADERS(fcntl.h malloc.h mcheck.h sys/resource.h malloc/malloc.h)
AC_CHECK_HEADERS(resource.h sys/resource.h)
AC_CHECK_HEADERS(sys/types.h sys/wait.h sys/stat.h sys/file.h signal.h)
AC_CHECK_HEADERS(sys/utsname.h sys/vfs.h sys/statfs.h sys/sysinfo.h)
AC_CHECK_HEADERS(obstack.h)

AC_CHECK_MEMBERS([struct rusage.ru_inblock, struct rusage.ru_nvcsw, struct rusage.ru_nivcsw],[],[],[#include <sys/resource.h>])
AC_CHECK_MEMBERS([struct rusage.ru_majflt, struct rusage.ru_nswap],[],[],[#include <sys/resource.h>])
AC_CHECK_MEMBERS([struct rusage.ru_inblock, struct rusage.ru_nvcsw, struct rusage.ru_nivcsw],[],[],[#include <sys/resource.h>])
AC_CHECK_MEMBERS([struct stat.st_mtim, struct stat.st_mtimespec],[],[],[#include <sys/stat.h>])
AC_CHECK_DECLS([RUSAGE_THREAD],[],[],[#include <sys/resource.h>])

dnl Various directory/path configuration options

AC_ARG_WITH(share_dir,--with-share-dir location,,with_share_dir=$prefix/share/kno)
AC_DEFINE_UNQUOTED(KNO_SHARE_DIR,"$with_share_dir")
share_dir=$with_share_dir

AC_ARG_WITH(data_dir,--with-data-dir location,,with_data_dir=$share_dir/data)
AC_DEFINE_UNQUOTED(KNO_DATA_DIR,"$with_data_dir")
data_dir=$with_data_dir

if test $prefix == "/usr"; then
   default_config_dir="/etc/kno${suffix}";
   default_buildopts_dir="/etc/beingmeta/opts";
else
   default_config_dir="$prefix/etc/kno${suffix}";
   default_buildopts_dir="$prefix/etc/beingmeta/opts";
fi

AC_ARG_WITH(config_dir,--with-config-dir location,,with_config_dir=$default_config_dir)
config_dir=$with_config_dir

AC_ARG_WITH(buildopts,--with-buildopts-dir location used by getbuildopt,,with_buildopts_dir=$default_buildopts_dir)
buildopts_dir=$with_buildopts_dir

AC_ARG_WITH(local_config,--with-local-config location,,with_local_config=$config_dir/config)
local_config=$with_local_config
AC_ARG_WITH(shared_config,--with-shared-config location,,with_shared_config=$share_dir/config)
shared_config=$with_shared_config

AC_DEFINE_UNQUOTED(KNO_CONFIG_FILE_PATH,"$with_local_config/%:$with_shared_config/%")

AC_ARG_WITH(unpackage_dir,--with-unpackage-dir location,,with_unpackage_dir=$share_dir/packages)
unpackage_dir=$with_unpackage_dir

dnl This builds in file config lookups
AC_ARG_ENABLE(fileconfig,--en/disable-fileconfig enable file-based configuration,,enable_fileconfig="yes")
if test "$enable_fileconfig" = "yes"; then
  AC_DEFINE(KNO_FILECONFIG_ENABLED,1)
  AC_DEFINE(KNO_FILECONFIG_DEFAULTS,1)
elif test "$enable_fileconfig" = "secure"; then
  AC_DEFINE(KNO_FILECONFIG_ENABLED,1)
  AC_DEFINE(KNO_FILECONFIG_DEFAULTS,0)
else
  AC_DEFINE(KNO_FILECONFIG_ENABLED,0)
  AC_DEFINE(KNO_FILECONFIG_DEFAULTS,0)
fi

AC_ARG_ENABLE(xprofiling,--enable-xprofiling use xprofiling information,enable_xprofiling=$withval,enable_xprofiling="yes")
if test "${enable_xprofiling}" != "no"; then
   AC_DEFINE(KNO_EXTENDED_PROFILING,1)
else
   AC_DEFINE(KNO_EXTENDED_PROFILING,0)
fi;

### Scheme Module search paths

AC_ARG_WITH(installed_module_dir,--with-installed-module-dir location,,with_installed_module_dir=$share_dir/modules/installed)
installed_module_dir=$with_installed_module_dir

AC_ARG_WITH(local_modules,--with-local-modules location,,with_local_modules=$share_dir/modules/local)
local_module_dir=$with_local_modules

AC_ARG_WITH(stdlib_module_dir,--with-stdlib-module-dir location,,with_stdlib_module_dir=$share_dir/modules/stdlib)
stdlib_module_dir=$with_stdlib_module_dir

LIBSCM="${share_dir}/libscm/\${REVISION}"

AC_ARG_WITH(loadpath,--with-loadpath location)

AC_ARG_WITH(libscm,--with-libscm location)
if test "$with_libscm" == "version"; then
   AC_DEFINE_UNQUOTED(KNO_LIBSCM_DIR,"$share_dir/libscm/kno-$KNO_VERSION")
   LIBSCM="$share_dir/libscm/kno-$KNO_VERSION";
elif test ! -z "$with_libscm"; then
   AC_DEFINE_UNQUOTED(KNO_LIBSCM_DIR,"$with_libscm")
   LIBSCM=$with_libscm
fi;

stdlib_search_dir=
AC_ARG_WITH(stdlib,--with-stdlib location)
if test -z "$with_stdlib"; then
   stdlib_search_dir=$stdlib_module_dir;
elif test ! -d "$with_stdlib"; then
   echo "No stdlib dir $with_stdlib";
   stdlib_search_dir=$stdlib_module_dir;
else 
   stdlib_search_dir=$with_stdlib;
fi;

stdlib_module_path=$stdlib_search_dir/

installed_module_path=$installed_module_dir/

local_module_path=$local_module_dir/

default_module_path=$local_module_path:$installed_module_path:$stdlib_module_path

AC_ARG_WITH(module_path,--with-module-path location,,with_module_path=$default_module_path)
module_path=$with_module_path

if test ! -z ${with_loadpath}; then
   module_path="${with_loadpath}:${module_path}";
fi;

AC_DEFINE_UNQUOTED(KNO_LOCAL_MODULE_DIR,"$local_module_dir")
AC_DEFINE_UNQUOTED(KNO_INSTALLED_MODULE_DIR,"$installed_module_dir")
AC_DEFINE_UNQUOTED(KNO_STDLIB_MODULE_DIR,"$stdlib_module_dir")
AC_DEFINE_UNQUOTED(KNO_UNPACKAGE_DIR,"$unpackage_dir")

AC_DEFINE_UNQUOTED(KNO_DEFAULT_LOADPATH,"$module_path")

AC_ARG_WITH(boot_config,--with-boot-config configstring,,with_boot_config="")
AC_DEFINE_UNQUOTED(KNO_BOOT_CONFIG,"$with_boot_config")

if test $exec_prefix = "NONE"; then
  AC_DEFINE_UNQUOTED(KNO_EXEC,"$prefix/bin/knox")
  AC_DEFINE_UNQUOTED(KNO_DBSERVER,"$prefix/bin/knod")
else
  AC_DEFINE_UNQUOTED(KNO_EXEC,"$exec_prefix/bin/knox")
  AC_DEFINE_UNQUOTED(KNO_DBSERVER,"$prefix/bin/knod")
fi

### Dynamic library load paths

AC_DEFINE_UNQUOTED(KNO_DLOAD_SUFFIX,"$shared_suffix")

AC_ARG_WITH(dload_path,--with-dload-path location,,with_dload_path=${DEFAULT_DLOADPATH})
AC_DEFINE_UNQUOTED(KNO_DEFAULT_DLOADPATH,"$with_dload_path")

### Miscellaneous functions

AC_CHECK_FUNCS(sigprocmask)
AC_CHECK_FUNCS(sigsetmask)
AC_CHECK_FUNCS(strdup strndup)
AC_CHECK_FUNCS(open_memstream)
AC_CHECK_FUNCS(mstats mtrace muntrace mallinfo malloc_info)
AC_CHECK_FUNCS(flock lockf fcntl)
AC_CHECK_FUNCS(sysconf sysctl sysinfo)
AC_CHECK_FUNCS(uname)
AC_CHECK_FUNCS(sleep)
AC_CHECK_FUNCS(nanosleep)
AC_CHECK_FUNCS(fseeko)
AC_CHECK_FUNCS(mmap)
AC_CHECK_FUNCS(pread)
AC_CHECK_FUNCS(ftime)
AC_CHECK_FUNCS(waitpid)
AC_CHECK_FUNCS(wait4)
AC_CHECK_FUNCS(statfs)
AC_CHECK_FUNCS(pipe pipe2)
AC_CHECK_FUNCS(gettext)

AC_CHECK_FUNCS(geteuid getuid getgid getegid getumask)
AC_CHECK_FUNCS(seteuid setuid setgid setegid)
AC_CHECK_FUNCS(getpwnam_r getpwnam)
AC_CHECK_FUNCS(getgrnam getgrnam_r)

AC_CHECK_LIB(pthread,pthread_rwlock_wrlock)
AC_CHECK_FUNCS(pthread_tryjoin_np pthread_timedjoin_np)

# This gets it in the library list
AC_CHECK_LIB(m,floor)
AC_CHECK_LIB(z,compress2)

### Dynamic library support

# Ditto, though it's almost always there
AC_CHECK_FUNCS(dlopen,libc_dlopen="yes")
if test "$libc_dlopen" != "yes"; then
   AC_CHECK_LIB(dl,dlopen)
fi

AC_CHECK_HEADERS(dlfcn.h)

AC_CHECK_LIB(dl,dladdr)
AC_CHECK_FUNCS(dladdr)

dnl Build the shared libraries.
AC_ARG_ENABLE(shared,--en/disable-shared Build with/without shared libraries,,enable_shared=$shared_default)
if test $enable_shared = "yes"; then
  CFLAGS="-fPIC $CFLAGS"
  SHARED_LIB_TARGET=shared-libs
else
  SHARED_LIB=
  SHARED_LIBV=
  SHARED_LIB_TARGET=
fi

AC_ARG_ENABLE(maintainer-mode,--en/disable-maintainer-mode doesn't do anything)
AC_ARG_ENABLE(dependency-tracking,--en/disable-dependency-tracking doesn't do anything)

dnl Enable async I/O features
AC_ARG_ENABLE(async,--en/disable-async Enable async I/O features)

if test "$enable_async" = "yes"; then
  DEFAULTISASYNC=1
  AC_DEFINE(KNO_DEFAULT_ASYNC,1)
else
  DEFAULTISASYNC=0
  AC_DEFINE(KNO_DEFAULT_ASYNC,0)
fi

dnl Enable dtblock for improved performance
AC_ARG_ENABLE(dtblock,--en/disable-dtblock DTBlock dtype for net I/O,,enable_dtblock=$use_dtblock_default)

if test "$enable_dtblock" = "yes"; then
  USEDTBLOCK=1
  AC_DEFINE(KNO_USE_DTBLOCK,1)
else
  USEDTBLOCK=0
  AC_DEFINE(KNO_USE_DTBLOCK,0)
fi

### Internationalization

# Internationalization
AC_CHECK_HEADERS(libintl.h)
AC_CHECK_FUNCS(gettext,[doi18n="yes"])
if test "$libc_gettext" != "yes"; then
   AC_CHECK_LIB(intl,gettext)
fi

dnl Whether to build message catalogs
AC_ARG_WITH(i18n,--with/out-i18n built and install message catalogs,,with_i18n="no")
if test $with_i18n = "yes"; then
  AC_CHECK_FUNCS(gettext,[doi18n="yes"])
fi
if test $doi18n = "yes"; then
  I18N="i18n"
  INSTALLI18N="install-i18n"
fi

if test $doi18n = "yes"; then
  I18N="i18n"
  INSTALLI18N="install-i18n"
fi

### Using mmap (if available)

AC_ARG_WITH(mmap,--with/out-mmap build without using mmap,,with_mmap="yes")
if test $with_mmap == "no"; then
   AC_DEFINE(KNO_WITHOUT_MMAP,1)
fi

### THREADS

AX_PTHREAD
AX_PTHREAD(threads_default="yes",threads_default="no")
AX_ATOMICS

dnl This forces the use of the threadlocal storage functions from the pthreads
dnl  library.  Otherwise, the executable will use __thread storage declarations
dnl  when available
AC_ARG_WITH(tls,--with/out-tls Force use of threadlocal storage,,with_tls=$tls_default)
if test $with_tls = "yes"; then
  AC_DEFINE(KNO_FORCE_TLS,1)
else
  AC_DEFINE(KNO_FORCE_TLS,0)
fi

dnl Size of the table used for hashlocking
AC_ARG_WITH(nptrlocks,--with-nptrlocks Sets the number of pointer hash locks to use)
if test ! -z $with_nptrlocks; then
   AC_DEFINE_UNQUOTED(KNO_N_PTRLOCKS,$with_nptrlocks)
fi

AC_ARG_ENABLE(thread_debug,--enable-thread-debug Use libu8 thread tracing and debugging,enable_thread_debug=$enableval,enable_thread_debug="no")

if test "${enable_thread_debug}" != "no"; then
   AC_DEFINE(U8_THREAD_DEBUG,1)
fi

default_enable_lockfree="yes";

AC_ARG_ENABLE(lockfree,--en/disable-lockfree Enables lock-free reference counting,,enable_lockfree=$default_enable_lockfree)

if test "$enable_lockfree" = "yes" && test "$ax_have_atomics" != "no"; then
  AC_DEFINE(KNO_ENABLE_LOCKFREE,1)
fi;

dnl ***********************************
dnl *** Checks for working __thread ***
dnl ***********************************
AC_MSG_CHECKING(for working __thread)
AC_TRY_COMPILE([
            #include <pthread.h>
            __thread int i;
	    static int res1, res2;
	],[
	     void thread_main (void *arg)
	     {
		i = arg;
		sleep (1);
		if (arg == 1) res1 = (i == arg);
		else res2 = (i == arg);
	     }

	     int main () {
		pthread_t t1, t2;
 		i = 5;
		pthread_create (&t1, NULL, thread_main, 1);
		pthread_create (&t2, NULL, thread_main, 2);

		pthread_join (t1, NULL);
		pthread_join (t2, NULL);
		return !(res1 + res2 == 2);
		}
	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_THREAD_STORAGE_CLASS)],
	       [AC_MSG_RESULT(no)])


### Use editline in kno console

dnl Use the editline library from kno console
AC_ARG_WITH(editline,--with/out-editline Use the editline with the kno console,,with_editline="yes")
if test "$with_editline" = "yes"; then
  AC_CHECK_HEADERS(editline/readline.h histedit.h)
  AC_CHECK_LIB(edit,el_init)
  AC_DEFINE(KNO_WITH_EDITLINE,1)
else
  AC_DEFINE(KNO_WITH_EDITLINE,0)
fi

### Alternate libu8 directory

dnl Use a different directory for libu8
AC_ARG_WITH(libu8,--with/out-libu8 Use libu8 includes,,with_libu8="")
if test "$with_libu8" != ""; then
  CFLAGS="-I$with_libu8/include $CFLAGS"
  LDFLAGS="-L$with_libu8/lib $LDFLAGS"
  EXEFLAGS="$EXEFLAGS -L$with_libu8/lib"
fi

dnl check for building with parseltongue
AC_ARG_WITH(parseltongue,--with/out-parseltongue Build parseltongue libraries for connecting Kno and Python,,with_parseltongue=auto)

if test "$with_parseltongue" = "no" || test "$with_parseltongue" = "2"; then
   echo "Not building parseltongue3 library";
elif test ! -z "$pkg_config" && $pkg_config --exists python3-embed; then
   PYTHON3_LDFLAGS=`$pkg_config python3-embed --libs`;
   PYTHON3_CFLAGS=`$pkg_config python3-embed --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python3; then
   PYTHON3_LDFLAGS=`$pkg_config python3 --libs`;
   PYTHON3_CFLAGS=`$pkg_config python3 --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python-3.9; then
   PYTHON3_LDFLAGS=`$pkg_config python-3.9 --libs`;
   PYTHON3_CFLAGS=`$pkg_config python-3.9 --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python-3.8; then
   PYTHON3_LDFLAGS=`$pkg_config python-3.8 --libs`;
   PYTHON3_CFLAGS=`$pkg_config python-3.8 --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python-3.7; then
   PYTHON3_LDFLAGS=`$pkg_config python-3.7 --libs`;
   PYTHON3_CFLAGS=`$pkg_config python-3.7 --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python-3.6; then
   PYTHON3_LDFLAGS=`$pkg_config python-3.6 --libs`;
   PYTHON3_CFLAGS=`$pkg_config python-3.6 --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python-3.5; then
   PYTHON3_LDFLAGS=`$pkg_config python-3.5 --libs`;
   PYTHON3_CFLAGS=`$pkg_config python-3.5 --cflags`;
else
   AX_PYTHON3
   if test ! -z "${ax_python3_lib}"; then
     PYTHON3_CFLAGS="-I${PYTHON3_INCLUDE_DIR}";
     PYTHON3_LDFLAGS="-l${PYTHON3_LIB}";
   fi;
fi;	     

if test "$with_parseltongue" = "no" || test "$with_parseltongue" = "3"; then
   echo "Not building parseltongue2 library";
elif test ! -z "$pkg_config" && $pkg_config --exists python2; then
   PYTHON2_LDFLAGS=`$pkg_config python2 --libs`;
   PYTHON2_CFLAGS=`$pkg_config python2 --cflags`;
elif test ! -z "$pkg_config" && $pkg_config --exists python-2.7; then
   PYTHON2_LDFLAGS=`$pkg_config python-2.7 --libs`;
   PYTHON2_CFLAGS=`$pkg_config python-2.7 --cflags`;
else
   AX_PYTHON2
   if test ! -z "${ax_python2_lib}"; then
     PYTHON2_CFLAGS="-I${PYTHON2_INCLUDE_DIR}";
     PYTHON2_LDFLAGS="-l${PYTHON2_LIB}";
   fi;
fi;

if test ! -z "${PYTHON3_CFLAGS}"; then
   CMODULES="lib/kno/parseltongue.$shared_suffix ${CMODULES}"
   INSTALL_CMODULES="install-parseltongue ${INSTALL_CMODULES}"
else
   auto_disabled="parseltongue3 ${auto_disabled}";
fi
if test ! -z "${PYTHON2_CFLAGS}"; then
   CMODULES="lib/kno/parseltongue2.$shared_suffix ${CMODULES}"
   INSTALL_CMODULES="install-parseltongue2 ${INSTALL_CMODULES}"
else
   auto_disabled="parseltongue2 ${auto_disabled}";
fi

### Control use of 'sudo' for installation

dnl Use sudo for installations in makefile
AC_CHECK_PROGS(sudo_program,sudo,"na")
if test "$sudoprogram" == "na"; then
  SUDO="";
else
  SUDO=$sudo_program
fi;

AC_ARG_WITH(sudo,--with/out-sudo use sudo with install,,with_sudo="$SUDO")

if test "$with_sudo" == "yes"; then
   DOINSTALL="${SUDO} ";
elif test "$with_sudo" == "no"; then
   DOINSTALL="";
else
   DOINSTALL="$with_sudo ";
fi

### Build fastcgi executable if possible

with_fastcgi="yes";
AC_ARG_WITH(fastcgi,--with/out-fastcgi Use fastcgi,with_fastcgi=no,[AC_DEFINE(WITH_FASTCGI,1)])
if test "$with_fastcgi" != "no"; then
  AC_CHECK_HEADERS(fcgiapp.h)
  AC_CHECK_LIB(fcgi,FCGX_Init)
fi

### zlib/libz compression functions

AC_CHECK_HEADER(zlib.h,have_zlib_h="yes",have_zlib_h="no")
if (test "$have_zlib_h" = "yes"); then
  AC_CHECK_LIB(z,compress2,have_libz="yes",have_libz="no")
fi

if (test "$have_libz" = "yes"); then
  CMODULES="lib/kno/zlib.$shared_suffix ${CMODULES}"
  INSTALL_CMODULES="install-zlib ${INSTALL_CMODULES}"
  TEST_CMODULES="${TEST_CMODULES} zlib"
fi

AC_CHECK_HEADER(zlib.h,have_zlib_h="yes",have_zlib_h="no")
if test "${have_zlib_h}" = "yes"; then
   AC_CHECK_LIB(zlib,uncompress)
   AC_DEFINE(HAVE_ZLIB_H,1)
fi;

### Google SNAPPY compression library

AC_CHECK_HEADER(snappy-c.h,have_snappyc_h="yes",have_snappyc_h="no")
if test "${have_snappyc_h}" = "yes"; then
   AC_CHECK_LIB(snappy,snappy_compress)
   AC_DEFINE(HAVE_SNAPPYC_H,1)
fi;
LIBS="${LIBS} -lsnappy"

### ZSTD (compression wrapper)

AC_CHECK_HEADER(zstd.h,have_zstd_h="yes",have_zstd_h="no")
if test "${have_zstd_h}" = "yes"; then
   AC_CHECK_LIB(zstd,ZSTD_decompress)
   AC_DEFINE(HAVE_ZSTD_H,1)
   AC_CHECK_FUNCS(ZSTD_getFrameContentSize)
fi;

### archive library (manipulates various kinds of archives)

AC_CHECK_HEADERS(archive.h)
AC_CHECK_LIB(archive,archive_read_new)
if test "${ac_cv_header_archive_h}" == "yes" && \
   test "${ac_cv_lib_archive_archive_read_new}" == "yes"; then
  CMODULES="lib/kno/libarchive.${shared_suffix} ${CMODULES}"
  INSTALL_CMODULES="install-libarchive ${INSTALL_CMODULES}"
  TEST_CMODULES="${TEST_CMODULES} libarchive"
else
  missing="libarchive ${missing}"
fi

### SQLITE3

AC_CHECK_HEADER(sqlite3.h,have_sqlite3_h="yes",have_sqlite3_h="no")
sqlite3_ok="no"
sqlite3v2_ok="no"
sqlite_included=no
SQLITE3_LIBS=

if test $have_sqlite3_h = "yes"; then
  AC_DEFINE(HAVE_SQLITE3_H,1)
  AC_CHECK_LIB(sqlite3,sqlite3_open,sqlite3_ok="yes")
  AC_CHECK_LIB(sqlite3,sqlite3_open_v2,sqlite3v2_ok="yes")
  if test "$sqlite3_ok" = "yes"; then
    CMODULES="lib/kno/sqlite.$shared_suffix ${CMODULES}";
    INSTALL_CMODULES="install-sqlite ${INSTALL_CMODULES}";
    TEST_CMODULES="${TEST_CMODULES} sqlite"
    SQLITE3_LIBS=-lsqlite3
    sqlite_included=no
    if test "${sqlite3v2_ok}" == "yes"; then
      AC_DEFINE(HAVE_SQLITE3_V2,1)
    fi;
  fi;
fi

if test ! "${sqlite_included}" = "yes"; then
   missing="sqlite ${missing}";
fi   

### Boot systems

if test -d /etc/systemd; then
  default_bootsystem="systemd";
elif test -d /lib/systemd; then
  default_bootsystem="systemd";
elif test -d /etc/init; then
  default_bootsystem="upstart";
 elif test -d /etc/init.d; then
  default_bootsystem="sysv";
 else
  default_bootsystem="none"
fi

AC_ARG_WITH(bootsystem,--with-bootsystem Setup boot method,,with_bootsystem=$default_bootsystem)

if test "$with_bootsystem" != "none"; then
  BOOTSETUP="setup-$with_bootsystem"
fi

if test -d /lib/systemd/system;
  then default_systemd_loc=/lib/systemd/system;
elif test -d /usr/lib/systemd/system;
  then default_systemd_loc=/usr/lib/systemd/system;
elif test -d /etc/systemd/system;
  then default_systemd_loc=/etc/systemd/system;
else default_systemd_loc=/usr/lib/systemd/system;
fi

AC_ARG_WITH(systemd,--with-systemd Specify systemd install directory,,with_systemd=$default_systemd_loc)
systemd_loc=$with_systemd

if test "$prefix" != "/usr"; then
  CFLAGS="$CFLAGS -I$prefix/include"
  CPPFLAGS="$CPPFLAGS -I$prefix/include"
  LDFLAGS="$LDFLAGS -L$prefix/lib"
fi

### These are components to preload

AC_DEFINE_UNQUOTED(KNO_SCHEME_BUILTINS,$scheme_builtins_init)

if test $exec_prefix = 'NONE';
  then BINDIR="${prefix}/bin";
else BINDIR="${exec_prefix}/bin";
fi

dnl Try to init submodules if needed and possible
if (test ! -f src/stdlib/samplefns.scm); then
   if (which git 2>&1 > /dev/null) && (test -d .git); then
     git submodule update --init;
   else
     echo "Can't initialize git submodules";
   fi;
fi;

### Here's where stuff gets made

AC_OUTPUT([makefile etc/version etc/makefile.include
           src/extmods/makefile DIY/makefile 
	   tests/makefile tests/libtests/makefile
           etc/gdbinit
           etc/lldbinit
	   exe/.gdbinit
	   exe/.lldbinit
	   dbg/runenv.sh dbg/.gdbinit dbg/.lldbinit dbg/.valgrind
	   etc/kno.pc
	   etc/getbuildopt
	   etc/knobuild
	   etc/kno_start_servlets etc/kno_start_daemons etc/kno_stop_jobs
	   etc/knostart etc/knostop etc/knostartall etc/knostopall etc/knocleanup
	   etc/knosignal etc/knoinject etc/knostatus etc/knolog 
	   etc/knoreload etc/knorestart etc/knoctl etc/knogdb etc/knodbgexec
	   etc/knosetconfig etc/knogetconfig etc/knoconfig
	   tests/testenv.sh tests/xtest
	   tests/runtest tests/memtest
	   tests/leaktest tests/knoxtest
	   src/scripts/knopkg.scm
	   src/extmods/knoconfig
	   etc/knojob
	   etc/logrotate
	   etc/sysv/kno-rc.d
	   etc/sysv/ex_servlet-init.d
	   etc/sysv/ex_daemon-init.d
	   etc/upstart/kno.conf
	   etc/upstart/knod.conf
	   etc/upstart/knoservlets.conf
	   etc/upstart/ex_knoservlet.conf
	   etc/systemd/ex_servlet.service
	   etc/systemd/ex_daemon.service
	   etc/systemd/kno-daemons.service
	   etc/systemd/kno-servlets.service
	   etc/macos/OSXStartupItem/KNO
	   etc/macos/KNO.plist
	   etc/apache/knocgi.conf 
	   etc/apache/knocgi.load
	   etc/install-script 
	   dist/alpine/APKBUILD
	   dist/kno-4.3.spec],
	   [chmod a+x etc/kno* tests/*test etc/getbuildopt src/extmods/knoconfig])

if test ! -z "$gperftools"; then
   echo  "Built with malloc=$using_malloc, gperftools=${gperftools}";
else
   echo "Built with malloc=$using_malloc";
fi;
echo "Patch level is ${KNO_PATCHLEVEL}";
echo $using_malloc > ./.malloc;
if test ! -z "$manually_disabled"; then
   echo "+++ These components have been explicitly disabled: $manually_disabled"; fi
if test ! -z "$auto_disabled"; then
   echo "+++ These components can't be built: $auto_disabled"; fi
if test ! -z "$force_enabled"; then
   echo "+++ These components were enabled by force and may have issues: $force_enabled"; fi
if test "$enable_ipeval" = "yes"; then
   echo "+++ You've configured Kno to use iterated partial evaluation";
   echo "+++ Use of the resulting software may be restricted by patent law. ";
   echo "+++ Contact beingmeta (licensing@beingmeta.com) for more information.";
   echo "+++ Thanks for using Kno!";
else
   echo "+++ Thanks for using Kno!";
fi

# Emacs local variables
#  ;;;  Local variables: ***
#  ;;;  compile-command: "autoconf; configure;" ***
#  ;;;  End: ***
